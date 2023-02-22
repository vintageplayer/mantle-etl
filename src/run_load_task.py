import json
import logging
import os
import time
from datetime import datetime, timedelta
from tempfile import TemporaryDirectory

from google.cloud import bigquery
from google.cloud.bigquery import TimePartitioning, SchemaField

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

resources_folders = os.environ.get('resources_folders', '/Users/adityaagarwal/Programming/competitions/gitcoin/2023_mantle_mash/submissions/mantle-etl')
destination_dataset_project_id = 'causal-sum-378306'
output_bucket = 'mantle-etl'
chain='ethereum'
dataset_name = f'crypto_{chain}'
dataset_name_raw = f'crypto_{chain}_raw'
dataset_name_temp = f'crypto_{chain}_temp'
load_all_partitions = True

environment = {
    'dataset_name': dataset_name,
    'dataset_name_raw': dataset_name_raw,
    'dataset_name_temp': dataset_name_temp,
    'destination_dataset_project_id': destination_dataset_project_id,
    'load_all_partitions': load_all_partitions
}


def read_bigquery_schema_from_file(filepath):
    result = []
    file_content = read_file(filepath)
    json_content = json.loads(file_content)
    for field in json_content:
        result.append(bigquery.SchemaField(
            name=field.get('name'),
            field_type=field.get('type', 'STRING'),
            mode=field.get('mode', 'NULLABLE'),
            description=field.get('description')))
    return result

def read_file(filepath):
    with open(filepath) as file_handle:
        content = file_handle.read()
        return content

def submit_bigquery_job(job, configuration):
    try:
        logging.info('Creating a job: ' + json.dumps(configuration.to_api_repr()))
        result = job.result()
        logging.info(result)
        assert job.errors is None or len(job.errors) == 0
        return result
    except Exception:
        logging.info(job.errors)
        raise

def load_task(task, file_format, allow_quoted_newlines=False, **kwargs):
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig()
    schema_path = os.path.join(resources_folders, 'resources/stages/raw/schemas/{task}.json'.format(task=task))
    schema = read_bigquery_schema_from_file(schema_path)
    job_config.schema = schema
    job_config.source_format = bigquery.SourceFormat.CSV if file_format == 'csv' else bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.write_disposition = 'WRITE_TRUNCATE'
    job_config.allow_quoted_newlines = allow_quoted_newlines
    job_config.ignore_unknown_values = True

    export_location_uri = 'gs://{bucket}/export'.format(bucket=output_bucket)
    uri = '{export_location_uri}/{task}/*.{file_format}'.format(export_location_uri=export_location_uri, task=task, file_format=file_format)
    table_ref = client.dataset(dataset_name_raw).table(task)
    load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
    submit_bigquery_job(load_job, job_config)
    logging.info(f'Finished Loading: {task}')


def enrich_task(task, time_partitioning_field='block_timestamp', **kwargs):
    template_context = {}
    template_context['ds'] = ''
    template_context['params'] = environment

    client = bigquery.Client()

    # Need to use a temporary table because bq query sets field modes to NULLABLE and descriptions to null
    # when writeDisposition is WRITE_TRUNCATE

    # Create a temporary table
    temp_table_name = '{task}_{milliseconds}'.format(task=task, milliseconds=int(round(time.time() * 1000)))
    temp_table_ref = client.dataset(dataset_name_temp).table(temp_table_name)

    schema_path = os.path.join(resources_folders, 'resources/stages/enrich/schemas/{task}.json'.format(task=task))
    schema = read_bigquery_schema_from_file(schema_path)
    table = bigquery.Table(temp_table_ref, schema=schema)

    description_path = os.path.join(resources_folders, 'resources/stages/enrich/descriptions/{task}.txt'.format(task=task))
    table.description = read_file(description_path)
    if time_partitioning_field is not None:
        table.time_partitioning = TimePartitioning(field=time_partitioning_field)
    logging.info('Creating table: ' + json.dumps(table.to_api_repr()))
    table = client.create_table(table)
    assert table.table_id == temp_table_name

    # Query from raw to temporary table
    query_job_config = bigquery.QueryJobConfig()
    # Finishes faster, query limit for concurrent interactive queries is 50
    query_job_config.priority = bigquery.QueryPriority.INTERACTIVE
    query_job_config.destination = temp_table_ref

    sql_path = os.path.join(resources_folders, 'resources/stages/enrich/sqls/{task}.sql'.format(task=task))
    sql_template = read_file(sql_path)
    # sql = kwargs['task'].render_template(sql_template, template_context)
    sql = sql_template

    print('Enrichment sql:')
    print(sql)

    query_job = client.query(sql, location='US', job_config=query_job_config)
    submit_bigquery_job(query_job, query_job_config)
    assert query_job.state == 'DONE'

    if load_all_partitions:
        # Copy temporary table to destination
        copy_job_config = bigquery.CopyJobConfig()
        copy_job_config.write_disposition = 'WRITE_TRUNCATE'
        dest_table_name = '{task}'.format(task=task)
        dest_table_ref = client.dataset(dataset_name, project=destination_dataset_project_id).table(dest_table_name)
        copy_job = client.copy_table(temp_table_ref, dest_table_ref, location='US', job_config=copy_job_config)
        submit_bigquery_job(copy_job, copy_job_config)
        assert copy_job.state == 'DONE'
    # Delete temp table
    client.delete_table(temp_table_ref)


def run():
	load_task('blocks', 'json')
	load_task('transactions', 'json')
	load_task('receipts', 'json')
	load_task('logs', 'json')
	load_task('token_transfers', 'json')

	enrich_task('blocks', time_partitioning_field='timestamp')
	enrich_task('transactions', time_partitioning_field='timestamp')
	enrich_task('logs', time_partitioning_field='timestamp')
	enrich_task('token_transfers', time_partitioning_field='timestamp')

if __name__ == '__main__':
	run()