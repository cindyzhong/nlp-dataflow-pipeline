# Copyright 2020 Google LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# [START SAF pubsub_to_bigquery]

from __future__ import absolute_import
import argparse
import logging
import re
import json
import time

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions


# function to concatenate the text from live chat to mimic the transcript field

class ProcessJSON(beam.DoFn):

    def process(self, data):
        import datetime as dt
        filename = data[0]
        mydata = json.loads(data[1])

        # Get the concatenated live chat
        livechat_text = ""
        for entry in mydata['entries']:
            livechat_text = livechat_text + "[" + entry['role'] + "] " + entry['text'].strip() + ". "

        # Get the starttime and endtime of session
        starttime = int(mydata['entries'][0]['start_timestamp_usec'][0:10]) 
        starttime = dt.datetime.fromtimestamp(starttime)
        endtime = int(mydata['entries'][-1]['start_timestamp_usec'][0:10]) 
        endtime = dt.datetime.fromtimestamp(endtime)
        duration = round((endtime - starttime).total_seconds() / 60,2)

        date = starttime.date()
        year = starttime.year
        month = starttime.month
        day = starttime.day

        # Construct the dictionary to output
        processjson_output = {'starttime':str(starttime),
                  'endtime':str(endtime),
                  'duration': duration,
                  'date':str(date),
                  'year':year,
                  'month':month,
                  'day':day,
                  'transcript':livechat_text,
                  'filename':filename}

        return [processjson_output]


# function to get NLP Sentiment and Entity

class GetNLPOutput(beam.DoFn):

    def mapspeaker(self, sentence):
        if sentence.startswith('[CUSTOMER]'):
            out_sentence = sentence.strip('[CUSTOMER]').strip()
            out_speaker = 'CUSTOMER'
        elif sentence.startswith('[AGENT]'):
            out_sentence = sentence.strip('[AGENT]').strip()
            out_speaker = 'AGENT'
        else:
            out_sentence = sentence
            out_speaker = ''
        return {'sentence': out_sentence, 'speaker': out_speaker}

    def process(self, processjson_output):

        #mimic the schema of saf
        get_nlp_output_response = processjson_output
        get_nlp_output_response['sentences'] = []
        get_nlp_output_response['entities'] = []

        from oauth2client.client import GoogleCredentials
        from googleapiclient import discovery
        credentials = GoogleCredentials.get_application_default()
        nlp_service = discovery.build('language', 'v1beta2', credentials=credentials)

        # [START NLP analyzeSentiment]
        get_operation_sentiment = nlp_service.documents().analyzeSentiment(
            body={
                'document': {
                    'type': 'PLAIN_TEXT',
                    'content': processjson_output['transcript']
                }
            })
        response_sentiment = get_operation_sentiment.execute()

        get_nlp_output_response['sentimentscore'] = response_sentiment['documentSentiment']['score']
        get_nlp_output_response['magnitude'] = response_sentiment['documentSentiment']['magnitude']

        for element in response_sentiment['sentences']:
            mapspeaker_output = self.mapspeaker(element['text']['content'])
            get_nlp_output_response['sentences'].append({
                'sentence': mapspeaker_output['sentence'],
                'speaker' : mapspeaker_output['speaker'],
                'sentiment': element['sentiment']['score'],
                'magnitude': element['sentiment']['magnitude']
            })
        # [END NLP analyzeSentiment]


        # [START NLP analyzeEntitySentiment]
        get_operation_entity = nlp_service.documents().analyzeEntitySentiment(
            body={
                'document': {
                    'type': 'PLAIN_TEXT',
                    'content': processjson_output['transcript']
                }
            })
        response_entity = get_operation_entity.execute()

        for element in response_entity['entities']:
            get_nlp_output_response['entities'].append({
                'name': element['name'],
                'type': element['type'],
                'sentiment': element['sentiment']['score']
            })
        # [END NLP analyzeEntitySentiment]

        # handle API Rate Limit for NLP
        time.sleep(3)

        # print ("######################################################")
        # print ("FINISHED LOADING " + get_nlp_output_response['filename'])
        # print (get_nlp_output_response['transcript'])
        # print ("######################################################")

        return [get_nlp_output_response]


def run(argv=None, save_main_session=True):
    """The main function which creates the pipeline and runs it."""

    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--input',
        dest='input',
        required=True,
        help='the path to the GCS bucket gs://[MY_BUCKET].')

    parser.add_argument('--output_bigquery', required=True,
                        help='Output BQ table to write results to '
                             '"PROJECT_ID:DATASET.TABLE"')

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    
    p = beam.Pipeline(options=pipeline_options)

    # Read the JSON files
    # chats = p | 'ReadfromGCS' >> beam.io.ReadFromText(known_args.input)
    chats = p | 'ReadfromGCS' >> beam.io.textio.ReadFromTextWithFilename(known_args.input)

    # Process live chat data in sentence into a long string
    chat = chats | 'ProcessJSON' >> beam.ParDo(ProcessJSON())

    # Get NLP Sentiment and Entity response
    nlp_output = chat | 'NaturalLanguageOutput' >> beam.ParDo(GetNLPOutput(filename=known_args.input))

    # Write to BigQuery
    bigquery_table_schema = {
        "fields": [
        {
            "mode": "NULLABLE", 
            "name": "fileid", 
            "type": "STRING"
        }, 
        {
            "mode": "NULLABLE", 
            "name": "filename", 
            "type": "STRING"
        }, 
        {
            "mode": "NULLABLE", 
            "name": "callid", 
            "type": "STRING"
        }, 
        {
            "mode": "NULLABLE", 
            "name": "date", 
            "type": "STRING"
        }, 
        {
            "mode": "NULLABLE", 
            "name": "year", 
            "type": "INTEGER"
        }, 
        {
            "mode": "NULLABLE", 
            "name": "month", 
            "type": "INTEGER"
        }, 
        {
            "mode": "NULLABLE", 
            "name": "day", 
            "type": "INTEGER"
        }, 
        {
            "mode": "NULLABLE", 
            "name": "starttime", 
            "type": "STRING"
        },
        {
            "mode": "NULLABLE", 
            "name": "endtime", 
            "type": "STRING"
        }, 
        {
            "mode": "NULLABLE", 
            "name": "duration", 
            "type": "FLOAT"
        }, 
        {
            "mode": "NULLABLE", 
            "name": "silencesecs", 
            "type": "FLOAT"
        },
        {
            "mode": "NULLABLE", 
            "name": "sentimentscore", 
            "type": "FLOAT"
        },
        {
            "mode": "NULLABLE", 
            "name": "magnitude", 
            "type": "FLOAT"
        }, 
        {
            "mode": "NULLABLE", 
            "name": "silencepercentage", 
            "type": "INTEGER"
        }, 
        {
            "mode": "NULLABLE", 
            "name": "speakeronespeaking", 
            "type": "FLOAT"
        }, 
        {
            "mode": "NULLABLE", 
            "name": "speakertwospeaking", 
            "type": "FLOAT"
        }, 
        {
            "mode": "NULLABLE", 
            "name": "nlcategory", 
            "type": "STRING"
        }, 
        {
            "mode": "NULLABLE", 
            "name": "transcript", 
            "type": "STRING"
        }, 
        {
            "fields": [
            {
                "mode": "NULLABLE", 
                "name": "name", 
                "type": "STRING"
            }, 
            {
                "mode": "NULLABLE", 
                "name": "type", 
                "type": "STRING"
            }, 
            {
                "mode": "NULLABLE", 
                "name": "sentiment", 
                "type": "FLOAT"
            }
            ], 
            "mode": "REPEATED", 
            "name": "entities", 
            "type": "RECORD"
        }, 
        {
            "fields": [
            {
                "mode": "NULLABLE", 
                "name": "word", 
                "type": "STRING"
            }, 
            {
                "mode": "NULLABLE", 
                "name": "startSecs", 
                "type": "FLOAT"
            }, 
            {
                "mode": "NULLABLE", 
                "name": "endSecs", 
                "type": "FLOAT"
            }, 
            {
                "mode": "NULLABLE", 
                "name": "speakertag", 
                "type": "INTEGER"
            }, 
            {
                "mode": "NULLABLE", 
                "name": "confidence", 
                "type": "FLOAT"
            }
            ], 
            "mode": "REPEATED", 
            "name": "words", 
            "type": "RECORD"
        }, 
        {
            "fields": [
            {
                "mode": "NULLABLE", 
                "name": "sentence", 
                "type": "STRING"
            },
            {
                "mode": "NULLABLE", 
                "name": "speaker", 
                "type": "STRING"
            }, 
            {
                "mode": "NULLABLE", 
                "name": "sentiment", 
                "type": "FLOAT"
            }, 
            {
                "mode": "NULLABLE", 
                "name": "magnitude", 
                "type": "FLOAT"
            }
            ], 
            "mode": "REPEATED", 
            "name": "sentences", 
            "type": "RECORD"
            }
        ]
    }
     
    nlp_output | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
             known_args.output_bigquery,
             schema=bigquery_table_schema,
             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
             write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)

    #p.run().wait_until_finish()
    p.run()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)
    run()
# [END SAF pubsub_to_bigquery]



