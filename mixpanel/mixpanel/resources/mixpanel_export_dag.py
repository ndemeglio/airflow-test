from datetime import datetime, timedelta

from airflow.contrib.operators.gcs_to_bq import \
    GoogleCloudStorageToBigQueryOperator as GCSToBQ
from airflow import models
from airflow.utils.log.logging_mixin import LoggingMixin

# pylint: disable=import-error, invalid-name
from airflow.operators.mixpanel_plugin import JQLToCloudStorage, DeleteFileFromGCS


log = LoggingMixin.log

DAG_NAME = 'mixpanel-export'
TIME_INTERVAL_SECONDS = 120
SCHEDULE_INTERVAL = timedelta(seconds=TIME_INTERVAL_SECONDS)
START_DATE = datetime.today() - timedelta(days=2)
CATCHUP = False # Disable automatic backfill

# ds -> YYYY-MM-DD string
# execution_date -> datetime.datetime
# start,end -> lambdas to convert datetime -> timestamp in ms
EVENT_KEY_TEMPLATE = \
    'data/{{ ds_nodash }}-mixpanel_events_interval-{{ execution_date | start }}'
PEOPLE_KEY_TEMPLATE = \
    'data/{{ ds_nodash }}-mixpanel_people_interval-{{ execution_date | start }}'

DAG_VARIABLES_DICT = models.Variable.get(DAG_NAME, {}, deserialize_json=True)

GCS_DATA_BUCKET = DAG_VARIABLES_DICT.get(
    'gcs_bucket',
    'test_bucket'
)
GCP_PROJECT = DAG_VARIABLES_DICT.get(
    'gcp_project',
    'test_project'
)
GCP_DATASET = DAG_VARIABLES_DICT.get(
    'gcp_dataset',
    'test_dataset'
)
BQ_PROJECT_DATASET = DAG_VARIABLES_DICT.get(
    'bigquery_dataset',
    'test_bq_dataset'
)
MIXPANEL_EXPORT_EVENTS_TABLE = DAG_VARIABLES_DICT.get(
    'bigquery_event_table',
    'events_test'
)
MIXPANEL_EXPORT_PEOPLE_TABLE = DAG_VARIABLES_DICT.get(
    'people_table',
    'people_test'
)


# timestamp is given in seconds, we need milliseconds for Mixpanel
def get_time_interval_lower_limit(execution_time):
    """Round the datetime object to start of the minute"""
    ts_seconds = execution_time.replace(second=0, microsecond=0).timestamp()
    return int(ts_seconds) * 1000


def get_time_interval_upper_limit(execution_time):
    """Set the upper (exclusive) boundary to start of next minute"""
    ts_seconds = (execution_time.replace(second=0, microsecond=0)
                  + timedelta(seconds=TIME_INTERVAL_SECONDS)).timestamp()
    return int(ts_seconds)*1000


# functions to use in Jinja templates, ("filters")
USER_DEFINED_FILTERS = {
    'start': get_time_interval_lower_limit,
    'end': get_time_interval_upper_limit
}

# DAG-level parameters; made accessible in templates
PARAMS = {
    'gcp_project': GCP_PROJECT,
    'gcs_bucket': GCS_DATA_BUCKET,
    'gcp_dataset': GCP_DATASET,
    'bq_dataset': BQ_PROJECT_DATASET,
    'event_table': MIXPANEL_EXPORT_EVENTS_TABLE,
    'event_key': EVENT_KEY_TEMPLATE,
    'people_table': MIXPANEL_EXPORT_PEOPLE_TABLE,
    'people_key': PEOPLE_KEY_TEMPLATE
}

# default args passed through to operators
DAG_DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': START_DATE,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'exponential_backoff': True,
    'retry_delay': timedelta(seconds=TIME_INTERVAL_SECONDS * 1.5),
    'project_id': GCP_PROJECT
}

with models.DAG(
        DAG_NAME,
        catchup=CATCHUP,
        default_args=DAG_DEFAULT_ARGS,
        user_defined_filters=USER_DEFINED_FILTERS,
        schedule_interval=SCHEDULE_INTERVAL) as event_dag:

    extract_events = JQLToCloudStorage(  # pylint: disable=invalid-name
        task_id='extract_events',
        mixpanel_conn_id='mixpanel',
        jql='templates/event_query_template.js',
        destination='GCS',
        dest_conn_id='google_cloud_storage_default',
        bucket=GCS_DATA_BUCKET,
        key=EVENT_KEY_TEMPLATE,
        params=PARAMS
    )
    load_events = GCSToBQ( # pylint: disable=invalid-name
        task_id='load_events',
        bigquery_conn_id='bigquery_default',
        google_cloud_storage_conn_id='google_cloud_storage_default',
        bucket=GCS_DATA_BUCKET,
        source_objects=[EVENT_KEY_TEMPLATE],
        destination_project_dataset_table=\
        '{{ params.bq_dataset }}.{{ params.event_table }}',
        schema_object='data/schemas/event_schema.json',
        source_format='NEWLINE_DELIMITED_JSON',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_APPEND',
        params=PARAMS
    )
    e_cleanup = DeleteFileFromGCS(
        task_id='clean_event_objects',
        gcs_conn_id='google_cloud_storage_default',
        bucket=GCS_DATA_BUCKET,
        key=EVENT_KEY_TEMPLATE
    )
    extract_events >> load_events >> e_cleanup

    extract_people = JQLToCloudStorage(  # pylint: disable=invalid-name
        task_id='extract_people',
        mixpanel_conn_id='mixpanel',
        jql='templates/people_query_template.js',
        destination='GCS',
        dest_conn_id='google_cloud_storage_default',
        bucket=GCS_DATA_BUCKET,
        key=PEOPLE_KEY_TEMPLATE,
        params=PARAMS
    )
    load_people = GCSToBQ(  # pylint: disable=invalid-name
        task_id='load_people',
        bigquery_conn_id='bigquery_default',
        google_cloud_storage_conn_id='google_cloud_storage_default',
        bucket=GCS_DATA_BUCKET,
        source_objects=[PEOPLE_KEY_TEMPLATE],
        schema_object='data/schemas/people_schema.json',
        destination_project_dataset_table=\
        '{{ params.bq_dataset }}.{{ params.people_table }}',
        source_format='NEWLINE_DELIMITED_JSON',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_APPEND',
        params=PARAMS
    )
    p_cleanup = DeleteFileFromGCS(
        task_id='clean_people_objects',
        gcs_conn_id='google_cloud_storage_default',
        bucket=GCS_DATA_BUCKET,
        key=PEOPLE_KEY_TEMPLATE
    )
    extract_people >> load_people >> p_cleanup
