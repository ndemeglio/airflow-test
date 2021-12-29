"""
Common quickbooks tasks that will be used
"""
import json
import logging
import os
import iso8601
import datetime
import re
from airflow.contrib.operators.bigquery_get_data import BigQueryGetDataOperator
import google
from google.cloud import bigquery
from quickbooks import Oauth1SessionManager, QuickBooks, exceptions as QbExceptions


QUICKBOOKS_BATCH_SIZE = 1000
BQ_BATCH_SIZE = 4000
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
CONSUMER_KEY = os.getenv('CONSUMER_KEY')
CONSUMER_SECRET = os.getenv('CONSUMER_SECRET')
ACCESS_TOKEN = os.getenv('ACCESS_TOKEN')
ACCESS_TOKEN_SECRET = os.getenv('ACCESS_TOKEN_SECRET')

ENG_CONN_ID = 'bigquery_acv_eng'
DATA_CONN_ID = 'my_gcp_connection'

NEWLINE_RE = re.compile(r'(\r\n|\r|\n)')
NON_ASCII_RE = re.compile(r'[^\x00-\x7F]+')


def get_access_token_and_secret():
    """
    Query the acv tokens table for the latest access token info
    :param session: sqlalchemy session
    :return: Tuple of access_token and token secret
    """
    logging.info('Get secret tokens from database')
    get_data = BigQueryGetDataOperator(
        bigquery_conn_id=ENG_CONN_ID,
        task_id='get_oauth_tokens_from_db',
        dataset_id='acv_prod',
        table_id='vendor_oauth_tokens',
        max_results='100',
        selected_fields='vendor_name,access_token,access_token_secret'
    )
    access_token = access_token_secret = None
    data = get_data.execute('Dummy Context')
    for row in data:
        vendor_name, access_token, access_token_secret = row
        if vendor_name == 'quickbooks':
            break

    return access_token, access_token_secret


def get_set_access_token_and_secret():
    """
    Passing the token into this app via an environment variable is handy but it may not be up-to-date. This updates
        those vars when called to the latest in the db
    :param session: sqlalchemy session
    """
    global ACCESS_TOKEN, ACCESS_TOKEN_SECRET
    logging.info('Get tokens and set token environment variables')
    access_token, access_token_secret = get_access_token_and_secret()
    os.environ['ACCESS_TOKEN'] = ACCESS_TOKEN = access_token
    os.environ['ACCESS_TOKEN_SECRET'] = ACCESS_TOKEN_SECRET = access_token_secret


def get_quickbooks_client():
    """
    Get the quickbooks client connection and verify that it works. If it fails on authentication get the latest auth
        from the database and try again.
    NOTE: If this continues to fail work with the other engineering teams and see if we can actually generate a new
        token ourselves and update that table or if they can make an api path for us to hit.

    :param session: sqlalchemy session
    :return: quickbooks client
    """
    logging.info('Get quickbooks client')

    QuickBooks.enable_global()

    def session_client():
        logging.info('Initialize QB session')
        session_manager = Oauth1SessionManager(
            consumer_key=CONSUMER_KEY,
            consumer_secret=CONSUMER_SECRET,
            access_token=ACCESS_TOKEN,
            access_token_secret=ACCESS_TOKEN_SECRET,
        )

        logging.info('Initialize QB client')

        client = QuickBooks(
            sandbox=False,
            session_manager=session_manager,
            company_id=392827956
        )
        return client

    # Test that this returns
    try:
        logging.info('Verify QB connection works')
        client = session_client()
        client.query(
            """
            SELECT id FROM Invoice MAXRESULTS 1
            """
        )
    except QbExceptions.AuthorizationException as e:
        # If it fails on authorization check to see if the new key is in the database
        # At this point that is the only recourse. If failures due to expired keys continues we will have to consider
        # re-authing ouselves here
        logging.exception(e)
        get_set_access_token_and_secret()
        logging.info('Test the QB connection after re-init')
        client = session_client()
        client.query(
            """
            SELECT id FROM Invoice MAXRESULTS 1
            """
        )
    else:
        return client


def write_to_bq(entities, table_name, schema):
    """
    Generate a csv of the quickbooks data and write the data to bigquery using the load_table_from_file method
    :param entities: list of formatted entity dictionaries (invoice/bill)
    :param table_name: The name of the entity table
    :param schema: List of bigquery table schema objects
    """
    total_count = len(entities)
    logging.info('Pushing {} to BQ'.format(total_count))
    client = bigquery.Client('acv-data')
    dataset = client.dataset('quickbooks')
    table = create_bq_table_if_not_exists(client, dataset, table_name, schema)
    for i in range(1, total_count, BQ_BATCH_SIZE):
        start = i
        end = i + BQ_BATCH_SIZE
        batch = entities[start:end]
        logging.info(
            'Pushing from {start} to {end} records to BQ from {table}'.format(
                start=start, end=end, table=table
            )
        )
        client.insert_rows_json(table, batch)


def create_bq_table_if_not_exists(client, dataset, table_name, schema):
    table_ref = dataset.table(table_name)
    try:
        exists = client.get_table(table_ref)
    except google.api_core.exceptions.NotFound:
        exists = False
    if not exists:
        table = bigquery.Table(table_ref, schema=schema)
        table.time_partitioning = bigquery.table.TimePartitioning(type_="DAY")
        table = client.create_table(table)
    else:
        table = exists

    assert table.table_id == table_name
    return table


def coerce_datetime_utc(dt):
    return datetime.datetime.replace(dt + dt.utcoffset(), tzinfo=None)


def format_value(value, schema_field=None):
    # valid values for date convert are DATE and DATETIME
    if schema_field in ('DATE', 'DATETIME'):
        value = iso8601.parse_date(value)
        return value.date().isoformat() if schema_field == 'DATE' else coerce_datetime_utc(value).isoformat()

    if isinstance(value, str):
        value = re.sub(NON_ASCII_RE, ' ', value)
        value = re.sub(NEWLINE_RE, ' ', value)
    elif isinstance(value, dict):
        value = format_value(
            json.dumps(
                {
                    key: format_value(r)
                    for key, r in value.items()
                }, ensure_ascii=True
            )
        )
    elif isinstance(value, list):
        value = [format_value(v) for v in value]
    else:
        value = value

    return value
