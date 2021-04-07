#!/usr/bin/env python
import datetime
# General config
#Edit this file
general_config = {
    "BUCKET": "<ADD_BUCKET_NAME>",
    "PROJECT_ID": "<ADD_PROJECT_NAME>",
}

# dbt config
dbt_config = {
    "DBT_PROJECT_DIR": "dbt/bq-dbt"
}
# sql export config
sql_export_config = {
    "DATASET": "<ADD_BUCKET_NAME>",
    "TABLE": "<ADD_TABLE_NAME>"
}

sql_query = {
    "QUERY": """EXPORT DATA
            OPTIONS(uri='gs://""" + general_config["BUCKET"] + """/stack_overflow*.json',
            overwrite = TRUE,
            format='JSON')
            AS
            SELECT * FROM `""" + general_config["PROJECT_ID"] + """.""" + sql_export_config["DATASET"] + """.""" +
             sql_export_config["TABLE"] + """`"""
}

# dataproc config
dataproc_config = {
    "REGION": "<ADD_REGION_NAME>",
    "ZONE_URI": "<ADD_ZONE_NAME>",
    "CLUSTER_NAME": "<ADD_CLUSTER_NAME>",
    "INIT_ACTIONS": "gs://dataproc-initialization-actions/python/pip-install.sh",
    "IMAGE": "1.5-debian10"
}
dataproc_create_cluster_config = {
    "projectId": general_config["PROJECT_ID"],
    "region": dataproc_config["REGION"],
    "clusterName": dataproc_config["CLUSTER_NAME"],
    "config": {
        "gceClusterConfig": {
            "zoneUri": dataproc_config["ZONE_URI"],
            "metadata": {
                "PIP_PACKAGES": "google-cloud-storage"
            },
        },
        "masterConfig": {
            "numInstances": "1",
            "machineTypeUri": "n1-standard-4",
        },
        "softwareConfig": {
            "imageVersion": dataproc_config["IMAGE"],
            "properties": {
                "dataproc:dataproc.allow.zero.workers": "true"
            },
            "optionalComponents": [
                "ANACONDA"
            ]
        },
        "initializationActions": [
            {
                "executableFile": dataproc_config["INIT_ACTIONS"]
            }
        ],
        "endpointConfig": {
            "enableHttpPortAccess": "true"
        },
    },
}

# pySPARK config
pyspark_config = {
    "PYTHON_FILE" : "gs://"+general_config["BUCKET"]+"/analyzeStackOverflow.py",
    "SPARK_BQ_JAR" : "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"
}
dataproc_pyspark_job_config = {
    "projectId": general_config["PROJECT_ID"],
    "job": {
        "placement": {"clusterName": dataproc_config["CLUSTER_NAME"]},
        "reference": {
            "jobId": "stack-pyspark-job" + datetime.datetime.now().strftime("%m%d%Y%H%M%S"),
            "projectId": general_config["PROJECT_ID"]
        },
        "pysparkJob": {
            "mainPythonFileUri": pyspark_config["PYTHON_FILE"],
            "jarFileUris": [
                pyspark_config["SPARK_BQ_JAR"]
            ]
        }
    }
}

