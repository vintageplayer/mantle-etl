import os
import logging
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

from ethereumetl.cli import (
    get_block_range_for_date,
    export_blocks_and_transactions,
    export_receipts_and_logs,
    extract_contracts,
    extract_tokens,
    extract_token_transfers,
    export_traces,
    extract_field,
)


GCS_DATA_DIR = "/Users/adityaagarwal/Programming/competitions/gitcoin/2023_mantle_mash/submissions/mantle-etl/data"
TEMP_DIR = GCS_DATA_DIR if Path(GCS_DATA_DIR).exists() else None
provider_uri = 'https://rpc.testnet.mantle.xyz/'

# Helps avoid OverflowError: https://stackoverflow.com/questions/47610283/cant-upload-2gb-to-google-cloud-storage
# https://developers.google.com/api-client-library/python/guide/media_upload#resumable-media-chunked-upload
def upload_to_gcs(gcs_hook, bucket, object, filename, mime_type='application/octet-stream'):
    from apiclient.http import MediaFileUpload
    from googleapiclient import errors

    service = gcs_hook.get_conn()
    bucket = service.get_bucket(bucket)
    blob = bucket.blob(object, chunk_size=10 * MEGABYTE)
    blob.upload_from_filename(filename)

# Can download big files unlike gcs_hook.download which saves files in memory first
def download_from_gcs(bucket, object, filename):
    from google.cloud import storage

    storage_client = storage.Client()

    bucket = storage_client.get_bucket(bucket)
    blob_meta = bucket.get_blob(object)

    if blob_meta.size > 10 * MEGABYTE:
        blob = bucket.blob(object, chunk_size=10 * MEGABYTE)
    else:
        blob = bucket.blob(object)

    blob.download_to_filename(filename)


# Export
def export_path(directory, date):
    return "export/{directory}/block_date={block_date}/".format(
        directory=directory, block_date=date.strftime("%Y-%m-%d")
    )

def copy_to_export_path(file_path, export_path):
    logging.info('Calling copy_to_export_path({}, {})'.format(file_path, export_path))
    filename = os.path.basename(file_path)
    upload_to_gcs(
        gcs_hook=cloud_storage_hook,
        bucket=output_bucket,
        object=export_path + filename,
        filename=file_path)

def copy_from_export_path(export_path, file_path):
    logging.info('Calling copy_from_export_path({}, {})'.format(export_path, file_path))
    filename = os.path.basename(file_path)
    download_from_gcs(bucket=output_bucket, object=export_path + filename, filename=file_path)

def get_block_range(tempdir, date):
    logging.info('Calling get_block_range_for_date({}, {}, ...)'.format(provider_uri, date))
    get_block_range_for_date.callback(
        provider_uri=provider_uri, date=date, output=os.path.join(tempdir, "blocks_meta.txt")
    )

    with open(os.path.join(tempdir, "blocks_meta.txt")) as block_range_file:
        block_range = block_range_file.read()
        start_block, end_block = block_range.split(",")

    return int(start_block), int(end_block)

def export_blocks_and_transactions_command(logical_date):
    with TemporaryDirectory(dir=TEMP_DIR) as tempdir:
        start_block, end_block = get_block_range(tempdir, logical_date, provider_uri)

        logging.info('Calling export_blocks_and_transactions({}, {}, {}, {}, {}, ...)'.format(
            start_block, end_block, export_batch_size, provider_uri, export_max_workers))

        export_blocks_and_transactions.callback(
            start_block=start_block,
            end_block=end_block,
            batch_size=export_batch_size,
            provider_uri=provider_uri,
            max_workers=export_max_workers,
            blocks_output=os.path.join(tempdir, "blocks.json"),
            transactions_output=os.path.join(tempdir, "transactions.json"),
        )

        copy_to_export_path(
            os.path.join(tempdir, "blocks_meta.txt"), export_path("blocks_meta", logical_date)
        )

        copy_to_export_path(
            os.path.join(tempdir, "blocks.json"), export_path("blocks", logical_date)
        )

        copy_to_export_path(
            os.path.join(tempdir, "transactions.json"), export_path("transactions", logical_date)
        )

def export_receipts_and_logs_command(logical_date):
    with TemporaryDirectory(dir=TEMP_DIR) as tempdir:
        copy_from_export_path(
            export_path("transactions", logical_date), os.path.join(tempdir, "transactions.json")
        )

        logging.info('Calling extract_csv_column(...)')
        extract_field.callback(
            input=os.path.join(tempdir, "transactions.json"),
            output=os.path.join(tempdir, "transaction_hashes.txt"),
            field="hash",
        )

        logging.info('Calling export_receipts_and_logs({}, ..., {}, {}, ...)'.format(
            export_batch_size, provider_uri, export_max_workers))
        export_receipts_and_logs.callback(
            batch_size=export_batch_size,
            transaction_hashes=os.path.join(tempdir, "transaction_hashes.txt"),
            provider_uri=provider_uri,
            max_workers=export_max_workers,
            receipts_output=os.path.join(tempdir, "receipts.json"),
            logs_output=os.path.join(tempdir, "logs.json"),
        )

        copy_to_export_path(
            os.path.join(tempdir, "receipts.json"), export_path("receipts", logical_date)
        )
        copy_to_export_path(os.path.join(tempdir, "logs.json"), export_path("logs", logical_date))

def extract_token_transfers_command(logical_date):
    with TemporaryDirectory(dir=TEMP_DIR) as tempdir:
        copy_from_export_path(
            export_path("logs", logical_date), os.path.join(tempdir, "logs.json")
        )

        logging.info('Calling extract_token_transfers(..., {}, ..., {})'.format(
            export_batch_size, export_max_workers
        ))
        extract_token_transfers.callback(
            logs=os.path.join(tempdir, "logs.json"),
            batch_size=export_batch_size,
            output=os.path.join(tempdir, "token_transfers.json"),
            max_workers=export_max_workers,
            values_as_strings=True,
        )

        copy_to_export_path(
            os.path.join(tempdir, "token_transfers.json"),
            export_path("token_transfers", logical_date),
        )

def run(logical_date):
	export_blocks_and_transactions_command(logical_date)
	export_receipts_and_logs_command(logical_date)
	extract_token_transfers_command(logical_date)


if __name__ == "__main__":
	run('2022-12-20T00:00:00')