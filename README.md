# mantle-etl

A repo dedicated to extracting data from Mantle Blockchain for analytical usec-cases.
Currently it uploads the data to google cloud storage and loads the data into big query tables for querying.

## Raw Data Files

Blocks: https://storage.googleapis.com/mantle-etl/export/blocks/block_date%3D2022-12-29/blocks.json

Transactions: https://storage.googleapis.com/mantle-etl/export/transactions/block_date%3D2022-12-29/transactions.json

Receipts: https://storage.googleapis.com/mantle-etl/export/receipts/block_date%3D2022-12-29/receipts.json

Logs: https://storage.googleapis.com/mantle-etl/export/logs/block_date%3D2022-12-29/logs.json

Token_Transfers: https://storage.googleapis.com/mantle-etl/export/token_transfers/block_date%3D2022-12-29/token_transfers.json

You can edit change the date in the url to access data for a specific date. This is the testnet data and for certain dates there were no transactions found. The data is present from the inception (29th December 2022) to 21st Feb 2023, if found.

## Public DataSet
The raw data is ingested in the dataset with ID: causal-sum-378306.crypto_ethereum_raw
It has been granted public view access.

Tables are blocks, transactions, logs, receipts and token_transfers.