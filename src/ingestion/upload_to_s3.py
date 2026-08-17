import argparse
import re
from pathlib import Path, PurePosixPath

import boto3
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError


DEFAULT_BUCKET = "nyc-taxi-aws-lakehouse-us-east-1"
DEFAULT_REGION = "us-east-1"

FILENAME_PATTERN = re.compile(
    r"^(?P<taxi_type>[a-z]+)_tripdata_"
    r"(?P<year>\d{4})-"
    r"(?P<month>0[1-9]|1[0-2])"
    r"\.parquet$",
    re.IGNORECASE,
)


def discover_parquet_files(source: Path) -> list[Path]:
    source = source.resolve()

    if source.is_file():
        return [source]

    if source.is_dir():
        return sorted(
            path
            for path in source.rglob("*.parquet")
            if path.is_file()
        )

    raise FileNotFoundError(f"Source does not exist: {source}")


def build_object_key(file_path: Path, layer: str) -> str:
    match = FILENAME_PATTERN.fullmatch(file_path.name)

    if not match:
        raise ValueError(
            f"Invalid NYC Taxi filename: {file_path.name}. "
            "Expected something like yellow_tripdata_2009-01.parquet"
        )

    taxi_type = match.group("taxi_type").lower()
    year = match.group("year")
    month = match.group("month")

    return str(
        PurePosixPath(
            layer.strip("/"),
            taxi_type,
            f"year={year}",
            f"month={month}",
            file_path.name,
        )
    )


def upload_directory(
    source: Path,
    bucket: str,
    layer: str,
    dry_run: bool,
) -> None:
    files = discover_parquet_files(source)

    if not files:
        print(f"No Parquet files found under: {source}")
        return

    s3_client = boto3.client("s3", region_name=DEFAULT_REGION)

    uploaded = 0
    skipped = 0
    failed = 0

    print(f"Discovered {len(files)} Parquet file(s).")
    print()

    for file_path in files:
        try:
            object_key = build_object_key(file_path, layer)
        except ValueError as error:
            skipped += 1
            print(f"SKIPPED: {error}")
            continue

        destination = f"s3://{bucket}/{object_key}"

        if dry_run:
            print(f"[DRY RUN] {file_path}")
            print(f"          -> {destination}")
            continue

        try:
            print(f"Uploading: {file_path.name}")
            print(f"       To: {destination}")

            s3_client.upload_file(
                Filename=str(file_path),
                Bucket=bucket,
                Key=object_key,
            )

            uploaded += 1

        except (ClientError, BotoCoreError) as error:
            failed += 1
            print(f"FAILED: {file_path}")
            print(f"Reason: {error}")

    print()
    print("Upload summary")
    print(f"Discovered: {len(files)}")
    print(f"Uploaded:   {uploaded}")
    print(f"Skipped:    {skipped}")
    print(f"Failed:     {failed}")

    if failed:
        raise SystemExit(1)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Upload NYC Taxi Parquet files to partitioned S3 prefixes."
    )

    parser.add_argument(
        "source",
        type=Path,
        help="A Parquet file or directory containing Parquet files",
    )

    parser.add_argument(
        "--bucket",
        default=DEFAULT_BUCKET,
        help=f"Destination bucket; default: {DEFAULT_BUCKET}",
    )

    parser.add_argument(
        "--layer",
        default="bronze",
        help="Lakehouse layer; default: bronze",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show generated keys without uploading",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_arguments()

    try:
        upload_directory(
            source=args.source,
            bucket=args.bucket,
            layer=args.layer,
            dry_run=args.dry_run,
        )
    except FileNotFoundError as error:
        print(f"File error: {error}")
        raise SystemExit(1)
    except NoCredentialsError:
        print("AWS credentials were not found. Run 'aws configure'.")
        raise SystemExit(1)


if __name__ == "__main__":
    main()