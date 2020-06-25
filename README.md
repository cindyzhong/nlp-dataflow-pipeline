# NLP Process Pipeline

This repository is adoped from the [Speech Analysis Framework] (https://github.com/GoogleCloudPlatform/dataflow-contact-center-speech-analysis)

It can and:
* Process JSON file on GCS Bucket
* Send the text in the JSON File to Cloud Natural Language APIs.
* Write the output to BigQuery.


The process follows:
1. JSON files being uploaded to GCS bucket
2. The Cloud Dataflow job picks up the files and sends the file to Cloud Natural Language APIs one by one
3. The Cloud Dataflow job writes the result to BigQuery

## How to install the Speech Analysis Framework

1. [Install the Google Cloud SDK](https://cloud.google.com/sdk/install)

2. Create a storage bucket for **Dataflow Staging Files**

``` shell
gsutil mb gs://[BUCKET_NAME]/
```

3. Through the [Google Cloud Console](https://console.cloud.google.com) create a folder named **tmp** in the newly created bucket for the DataFlow staging files

4. Create a storage bucket for **Uploaded JSON Files**

``` shell
gsutil mb gs://[BUCKET_NAME]/
```

5. Create a BigQuery Dataset
``` shell
bq mk [YOUR_BIG_QUERY_DATABASE_NAME]
```

6. Enable Cloud Dataflow API
``` shell
gcloud services enable dataflow
```

7. Enable Cloud Natural Language API
``` shell
gcloud services enable language.googleapis.com
```

8. Deploy the Cloud Dataflow Pipeline
* In the cloned repo, go to “saf-longrun-job-dataflow” directory and deploy the Cloud Dataflow Pipeline. Run the commands below to deploy the dataflow job.
``` shell
# Apple/Linux
python -m venv env
source env/bin/activate
pip install apache-beam[gcp]
pip install Cython
```
or
``` shell
# Windows
python -m venv env
env\Scripts\activate
pip install apache-beam[gcp]
pip install Cython
```
* The Dataflow job will create the **BigQuery Table** you listed in the parameters.
* Please wait as it might take a few minutes to complete.

* Running in Local Runner
``` shell
python3 gcs-nlp-bq-batch.py --project=[YOUR_PROJECT_ID]
--input=gs://[YOUR BUCKET]/*.json
--output_bigquery=[DATASET NAME].[TABLE] --requirements_file="requirements.txt"
```

* Running in Dataflow Runner
``` shell
python3 gcs-nlp-bq-batch.py --project=[YOUR_PROJECT_ID] --runner=DataflowRunner 
--input=gs://[YOUR BUCKET]/*.json
--temp_location=gs://[YOUR_DATAFLOW_STAGING_BUCKET]/tmp --output_bigquery=[DATASET NAME].[TABLE] --requirements_file="requirements.txt"
```

9. After a few minutes you will be able to see the data in BigQuery.
* Sample select statements that can be executed in the BigQuery console.
``` sql
-- Order Natural Language Entities for all records
SELECT
  *
FROM (
  SELECT
    entities.name,
    entities.type,
    COUNT(entities.name) AS count
  FROM
    `[YOUR_PROJECT_ID].[YOUR_DATASET].[YOUR_TABLE]`,
    UNNEST(entities) entities
  GROUP BY
    entities.name,
    entities.type
  ORDER BY
    count DESC )
```



``` sql
-- Search Transcript with a regular expression
SELECT
  transcript,
  sentimentscore,
  magnitude
FROM
  `[YOUR_PROJECT_ID].[YOUR_DATASET].[YOUR_TABLE]`
WHERE
  (REGEXP_CONTAINS(transcript, '(?i) [YOUR_WORD]' ))
```