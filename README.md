# Incremental ETL Pipeline

## Project Overview

This project implements an incremental ETL pipeline for a fictional payments aggregator, Lipa Haraka. It processes incoming JSON transaction batches, validates records, identifies new and updated transactions, handles duplicates, and maintains a CSV warehouse.

The pipeline uses a checkpoint to track processed batches and prevent unnecessary reprocessing.

## Project Structure

```text
day3-incremental-etl/
├── incremental_etl.py
├── data/
│   └── transactions.csv
├── incoming/
│   └── batch_2026-10-21.json
├── state/
│   └── checkpoint.json
├── logs/
│   └── etl.log
├── requirements.txt
├── .gitignore
└── README.md
```

## Technologies Used

- Python
- CSV and JSON processing
- Python logging
- File handling with `pathlib`
- Dictionaries and sets for transaction matching
- Atomic file replacement
- Git and GitHub

## How the Pipeline Works

1. Loads existing transactions from the CSV warehouse.
2. Reads incoming JSON batch files.
3. Validates transaction fields, amounts, and timestamps.
4. Classifies transactions as new, updated, duplicate, or invalid.
5. Prevents older transaction versions from overwriting newer records.
6. Merges valid new and updated transactions into the warehouse.
7. Saves the warehouse through a temporary file followed by replacement.
8. Updates the checkpoint after the warehouse has been saved.
9. Logs processing information and invalid records.

## Validation and Duplicate Handling

Records with missing required fields, invalid amounts, or invalid timestamps are rejected.

Transaction amounts must be greater than zero. Amount strings containing thousands separators are normalized before classification.

Transactions with identical compared fields are treated as duplicates. Older transaction versions do not overwrite newer warehouse records. Repeated transaction IDs are handled to avoid creating additional warehouse rows.

## Checkpoint

The checkpoint is stored in `state/checkpoint.json` and contains:

- `last_run_at`: timestamp of the last successful batch-processing update.
- `processed_batches`: identifiers of processed batches.
- `high_watermark`: latest transaction update timestamp in the warehouse.
- `row_count`: number of warehouse rows.

The checkpoint is written only after the warehouse save succeeds.

## Logging

The pipeline logs to the console and `logs/etl.log`. Logs include batch processing counts, invalid records, skipped batches, warehouse row counts, and execution duration.

## Testing and Results

The pipeline was tested using the supplied transaction warehouse and JSON batch.

### First run

| Metric | Result |
|---|---:|
| Existing warehouse rows | 20 |
| Incoming records | 17 |
| New transactions | 7 |
| Updated transactions | 3 |
| Duplicates | 5 |
| Invalid records | 2 |
| Final warehouse rows | 27 |

### Second run

The already processed batch was skipped. No new or updated transactions were applied, and the warehouse remained at 27 rows.

### Checkpoint deletion test

After deleting the checkpoint, the batch was processed again against the existing warehouse. The pipeline reported zero new transactions and zero updates. The warehouse remained at 27 rows with 27 unique transaction IDs.

## Running the Project

Use Python 3.10 or later.

From the project root, run:

```powershell
python incremental_etl.py
```

The pipeline reads JSON files from `incoming/`, updates `data/transactions.csv`, writes the checkpoint to `state/checkpoint.json`, and records logs in `logs/etl.log`.

## Dependencies

The pipeline uses Python's standard library and does not require third-party packages.

## Author

Whelma Bange Nyabuto
