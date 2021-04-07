import dagstermill as dm
from dagster import pipeline, solid, ModeDefinition, InputDefinition, Nothing, Field, OutputDefinition, String, solid
from dagster_dbt import dbt_cli_run
from dagster_gcp import bq_solid_for_queries
from dagster_gcp.dataproc.resources import DataprocResource
from dagster_gcp import bigquery_resource
from dagster.utils import script_relative_path
from google.cloud import storage
import pipeline_config as cfg


@solid(input_defs=[InputDefinition("start", Nothing)])
def create_dataproc_cluster(_):
    DataprocResource(cfg.dataproc_create_cluster_config).create_cluster()


@solid(input_defs=[InputDefinition("start", Nothing)])
def data_proc_spark_operator(context):
    cluster_resource = DataprocResource(cfg.dataproc_create_cluster_config)
    job = cluster_resource.submit_job(cfg.dataproc_pyspark_job_config)
    job_id = job["reference"]["jobId"]
    cluster_resource.wait_for_job(job_id)


@solid(input_defs=[InputDefinition("start", Nothing)])
def delete_dataproc_cluster(_):
    DataprocResource(cfg.dataproc_create_cluster_config).delete_cluster()


@solid(name="download_file",
    config_schema={
        "uri": Field(String, description="The GCS URI from which to download the file"),
        "filename": Field(String, description="Filename"),
        "path": Field(String, description="The path to which to download the file"),
    },
    input_defs=[InputDefinition("start", Nothing)],
    description=(
        "A simple utility solid that downloads a file from a URL to a path using "
        "urllib.urlretrieve"
    ),
)
def download_file(context):
    storage_client = storage.Client.from_service_account_json('service_account.json')
    bucket = storage_client.bucket(context.solid_config["uri"])
    blobs = bucket.list_blobs(prefix=context.solid_config["filename"])
    for blob in blobs:
        destination_uri = '{}/{}'.format(context.solid_config["path"], blob.name)
        blob.download_to_filename(destination_uri)


@pipeline(mode_defs=[ModeDefinition(resource_defs={"bigquery": bigquery_resource})])
def my_gcp_dataops_pipeline():

    sql_process = bq_solid_for_queries([cfg.sql_query["QUERY"]]).alias("export_bq_table_gcs")(dbt_cli_run.configured({"project-dir": cfg.dbt_config["DBT_PROJECT_DIR"]}, name="run_bq_dbt")())
    spark_process = delete_dataproc_cluster(data_proc_spark_operator(create_dataproc_cluster(sql_process)))
    jupyter_process = dm.define_dagstermill_solid("view_data_matplot", script_relative_path("jupyter/view_data.ipynb"),input_defs=[InputDefinition("start", Nothing)])(download_file(spark_process))
