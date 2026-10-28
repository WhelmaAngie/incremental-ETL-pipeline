"""
Day 3 - Incremental ETL Pipeline
Squad Python Specialization - Data Engineering

Run it twice in a row. The second run must change nothing.
"""
import csv
import json
import logging
import time
from datetime import datetime
from pathlib import Path


WAREHOUSE_PATH = Path("data") / "transactions.csv"
BATCH_DIR = Path("incoming")
CHECKPOINT_PATH = Path("state") / "checkpoint.json"
LOG_PATH = Path("logs") / "etl.log"

KEY = "transaction_id"
COMPARED_FIELDS = ("customer_id", "merchant", "amount_kes",
                   "status", "channel", "updated_at")


def setup_logging(path: Path = LOG_PATH) -> logging.Logger:
    """Log messages to both the console and a log file."""
    path.parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("incremental_etl")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    # Avoid adding duplicate handlers if logging is configured again
    if not logger.handlers:
        formatter = logging.Formatter(
            "%(asctime)s %(levelname)-7s %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)

        file_handler = logging.FileHandler(path, encoding="utf-8")
        file_handler.setFormatter(formatter)

        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

    return logger


def load_existing(path: Path = WAREHOUSE_PATH) -> dict[str, dict]:
    """Read the loaded transactions into a dict keyed by transaction_id."""
    existing = {}

    with path.open("r", newline="", encoding="utf-8-sig") as file:
        reader = csv.DictReader(file)

        for row in reader:
            transaction_id = row[KEY].strip()
            row["amount_kes"] = float(row["amount_kes"])
            existing[transaction_id] = row

    return existing


def load_batch(path: Path) -> tuple[str, list[dict]]:
    """Read one JSON batch file and return (batch_id, transactions)."""
    try:
        with path.open("r", encoding="utf-8") as file:
            data = json.load(file)

        batch_id = data["batch_id"]
        transactions = data["transactions"]

        if not isinstance(batch_id, str) or not batch_id.strip():
            raise ValueError("Missing or invalid batch_id")

        if not isinstance(transactions, list):
            raise ValueError("Batch transactions must be a list")

        return batch_id, transactions

    except (OSError, json.JSONDecodeError, KeyError, ValueError) as error:
        logging.getLogger(__name__).error(
            "Could not load batch %s: %s", path, error
        )
        raise


def validate_record(record: dict) -> list[str]:
    """Return reasons why a transaction is invalid."""
    errors = []

    # Check required fields
    required_fields = [
        "transaction_id",
        "customer_id",
        "merchant",
        "amount_kes",
        "status",
        "channel",
        "updated_at",
    ]

    for field in required_fields:
        value = record.get(field)

        if value is None or str(value).strip() == "":
            errors.append(f"Missing {field}")

    # Validate transaction amount
    amount = record.get("amount_kes")

    if amount is not None and str(amount).strip() != "":
        try:
            numeric_amount = float(amount.replace(
                ",", "") if isinstance(amount, str) else amount)

            if numeric_amount <= 0:
                errors.append("amount_kes must be greater than zero")

        except (ValueError, TypeError):
            errors.append("amount_kes must be numeric")

    # Validate timestamp
    updated_at = record.get("updated_at")

    if updated_at:
        try:
            datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            errors.append("Invalid updated_at timestamp")

    return errors


def classify(existing: dict[str, dict], batch: list[dict]) -> dict[str, list[dict]]:
    """Classify transactions as new, updated, duplicate, or invalid."""
    result = {
        "new": [],
        "updated": [],
        "duplicate": [],
        "invalid": [],
    }

    # Track IDs encountered in this batch
    seen_ids = set()

    # Keep the latest accepted version of each transaction in this batch
    batch_by_id = {}

    for record in batch:
        errors = validate_record(record)

        if errors:
            result["invalid"].append({
                "record": record,
                "reasons": errors,
            })
            continue

        transaction_id = str(record["transaction_id"]).strip()
        record["transaction_id"] = transaction_id

        # Convert amount to a number, including amounts written with commas
        amount = record["amount_kes"]
        if isinstance(amount, str):
            amount = amount.replace(",", "")
        record["amount_kes"] = float(amount)

        if transaction_id in seen_ids:
            previous = batch_by_id[transaction_id]

            if record == previous:
                result["duplicate"].append(record)
                continue

            incoming_time = datetime.fromisoformat(
                record["updated_at"].replace("Z", "+00:00")
            )
            previous_time = datetime.fromisoformat(
                previous["updated_at"].replace("Z", "+00:00")
            )

            if incoming_time > previous_time:
                batch_by_id[transaction_id] = record
            else:
                result["duplicate"].append(record)

            continue

        seen_ids.add(transaction_id)
        batch_by_id[transaction_id] = record

    # Compare each transaction's chosen batch version with the warehouse
    for transaction_id, record in batch_by_id.items():
        if transaction_id not in existing:
            result["new"].append(record)
            continue

        current = existing[transaction_id]

        same_fields = all(
            str(record[field]) == str(current[field])
            for field in COMPARED_FIELDS
        )

        if same_fields:
            result["duplicate"].append(record)
            continue

        incoming_time = datetime.fromisoformat(
            record["updated_at"].replace("Z", "+00:00")
        )
        current_time = datetime.fromisoformat(
            current["updated_at"].replace("Z", "+00:00")
        )

        if incoming_time > current_time:
            result["updated"].append(record)
        else:
            # Stale records must not overwrite newer warehouse data
            result["duplicate"].append(record)

    return result


def upsert(
    existing: dict[str, dict],
    new: list[dict],
    updated: list[dict],
) -> dict[str, dict]:
    """Merge new transactions and updates into the existing warehouse state."""

    merged = existing.copy()

    for record in new:
        transaction_id = record["transaction_id"]
        merged[transaction_id] = record

    for record in updated:
        transaction_id = record["transaction_id"]
        merged[transaction_id] = record

    return merged


def read_checkpoint(path: Path = CHECKPOINT_PATH) -> dict:
    """Read the saved checkpoint or return a fresh checkpoint."""
    if not path.exists():
        return {
            "last_run_at": None,
            "processed_batches": [],
            "high_watermark": None,
            "row_count": 0,
        }

    with path.open("r", encoding="utf-8") as file:
        checkpoint = json.load(file)

    return checkpoint


def write_checkpoint(
    checkpoint: dict,
    path: Path = CHECKPOINT_PATH,
) -> None:
    """Save checkpoint safely using a temporary file."""
    path.parent.mkdir(parents=True, exist_ok=True)

    temp_path = path.with_name(path.name + ".tmp")

    try:
        with temp_path.open("w", encoding="utf-8") as file:
            json.dump(checkpoint, file, indent=2)
            file.write("\n")

        temp_path.replace(path)

    finally:
        if temp_path.exists():
            temp_path.unlink()


def save_warehouse(
    state: dict[str, dict],
    path: Path = WAREHOUSE_PATH,
) -> None:
    """Save transactions in stable order using an atomic file replacement."""

    path.parent.mkdir(parents=True, exist_ok=True)

    temp_path = path.with_name(path.name + ".tmp")

    fieldnames = [
        "transaction_id",
        "customer_id",
        "merchant",
        "amount_kes",
        "status",
        "channel",
        "updated_at",
    ]

    try:
        with temp_path.open("w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()

            for transaction_id in sorted(state):
                record = state[transaction_id].copy()
                record["amount_kes"] = float(record["amount_kes"])
                writer.writerow(record)

        # Replace the original only after the temporary file is complete
        temp_path.replace(path)

    finally:
        # Clean up a leftover temporary file if saving fails
        if temp_path.exists():
            temp_path.unlink()


def run() -> dict:
    """Process unprocessed batches and safely update the warehouse."""
    start_time = time.perf_counter()
    logger = setup_logging()

    logger.info("Run started")

    existing = load_existing()
    logger.info("Existing rows loaded: %s", len(existing))

    checkpoint = read_checkpoint()
    processed_batches = set(checkpoint.get("processed_batches", []))

    batch_files = sorted(BATCH_DIR.glob("*.json"))

    total_new = 0
    total_updated = 0
    total_duplicates = 0
    total_invalid = 0
    processed_any = False

    for batch_path in batch_files:
        try:
            batch_id, transactions = load_batch(batch_path)
        except (OSError, json.JSONDecodeError, KeyError, ValueError):
            logger.error("Skipping unreadable batch file: %s", batch_path)
            continue

        if batch_id in processed_batches:
            logger.info("Batch %s already processed - skipping", batch_id)
            continue

        logger.info("Batch %s: %s records read", batch_id, len(transactions))

        classified = classify(existing, transactions)

        new = classified["new"]
        updated = classified["updated"]
        duplicates = classified["duplicate"]
        invalid = classified["invalid"]

        for item in invalid:
            record = item["record"]
            reasons = "; ".join(item["reasons"])
            record_id = record.get("transaction_id") or "unknown"
            logger.warning("Invalid record %s: %s", record_id, reasons)

        logger.info(
            "New: %s | Updated: %s | Duplicates: %s | Invalid: %s",
            len(new), len(updated), len(duplicates), len(invalid),
        )

        merged = upsert(existing, new, updated)

        # Save the warehouse before recording the batch in the checkpoint
        save_warehouse(merged)

        existing = merged
        processed_batches.add(batch_id)
        processed_any = True

        total_new += len(new)
        total_updated += len(updated)
        total_duplicates += len(duplicates)
        total_invalid += len(invalid)

        timestamps = [
            record["updated_at"]
            for record in existing.values()
            if record.get("updated_at")
        ]

        checkpoint["last_run_at"] = datetime.now().astimezone().isoformat()
        checkpoint["processed_batches"] = sorted(processed_batches)
        checkpoint["high_watermark"] = max(timestamps) if timestamps else None
        checkpoint["row_count"] = len(existing)

        write_checkpoint(checkpoint)

    if not processed_any:
        logger.info(
            "New: 0 | Updated: 0 | Duplicates: 0 | Invalid: 0"
        )

    logger.info("Rows after load: %s", len(existing))
    logger.info("Run finished in %.2fs", time.perf_counter() - start_time)

    return {
        "new": total_new,
        "updated": total_updated,
        "duplicates": total_duplicates,
        "invalid": total_invalid,
        "row_count": len(existing),
    }


if __name__ == "__main__":
    run()
