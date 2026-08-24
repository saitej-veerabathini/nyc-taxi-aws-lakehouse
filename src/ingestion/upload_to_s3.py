import argparse
import re
from pathlib import Path, PurePosixPath
import hashlib

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

def calculate_sha256(file_path, chunk_size: int = 8 * 1024 * 1024):
    """
    Calculate a SHA-256 checksum without loading the entire file into memory.
    """

    sha256 = hashlib.sha256()

    with file_path.open("rb") as file:
        while chunk := file.read(chunk_size):
            sha256.update(chunk)

    return sha256.hexdigest()

def get_remote_object_checksum(s3_client, bucket, object_key):
    """
    Return whether an S3 object exists and its stored SHA-256 checksum.
    """
    try:
        response = s3_client.head_object(
            Bucket=bucket,
            Key=object_key
        )
    except ClientError as error:
        error_code = error.response["Error"]["Code"]

        if error_code in {"404", "NoSuchKey", "NotFound"}:
            return False, None

        raise

    remote_checksum = response.get("Metadata", {}).get("sha256")

    return True, remote_checksum


def upload_directory(
    source: Path,
    bucket: str,
    layer: str,
    dry_run: bool,
    overwrite: bool,
) -> None:
    files = discover_parquet_files(source)

    if not files:
        print(f"No Parquet files found under: {source}")
        return

    s3_client = boto3.client(
        "s3",
        region_name=DEFAULT_REGION,
    )

    uploaded = 0
    unchanged = 0
    invalid = 0
    conflicts = 0
    failed = 0

    print(f"Discovered {len(files)} Parquet file(s).")
    print()

    for file_path in files:
        try:
            object_key = build_object_key(file_path, layer)
        except ValueError as error:
            invalid += 1
            print(f"INVALID: {error}")
            continue

        destination = f"s3://{bucket}/{object_key}"

        try:
            print(f"Checking: {file_path.name}")

            local_checksum = calculate_sha256(file_path)

            object_exists, remote_checksum = get_remote_object_checksum(
                s3_client=s3_client,
                bucket=bucket,
                object_key=object_key,
            )

            if object_exists and remote_checksum == local_checksum:
                unchanged += 1
                print(f"UNCHANGED: {destination}")
                print("           Checksums match; upload skipped.")
                continue

            if object_exists and not overwrite:
                conflicts += 1

                print(f"CONFLICT: {destination}")

                if remote_checksum is None:
                    print(
                        "          The object exists but has no stored checksum."
                    )
                else:
                    print(
                        "          The local and remote checksums are different."
                    )

                print(
                    "          Use --overwrite only after verifying "
                    "that replacement is intentional."
                )
                continue

            action = "overwrite" if object_exists else "upload"

            if dry_run:
                print(f"[DRY RUN] Would {action}: {file_path}")
                print(f"          Destination: {destination}")
                print(f"          SHA-256: {local_checksum}")
                continue

            print(f"Uploading: {file_path.name}")
            print(f"       To: {destination}")
            print(f"   SHA-256: {local_checksum}")

            s3_client.upload_file(
                Filename=str(file_path),
                Bucket=bucket,
                Key=object_key,
                ExtraArgs={
                    "Metadata": {
                        "sha256": local_checksum,
                    }
                },
            )

            uploaded += 1

        except (ClientError, BotoCoreError, OSError) as error:
            failed += 1
            print(f"FAILED: {file_path}")
            print(f"Reason: {error}")

    print()
    print("Ingestion summary")
    print(f"Discovered: {len(files)}")
    print(f"Uploaded:   {uploaded}")
    print(f"Unchanged:  {unchanged}")
    print(f"Invalid:    {invalid}")
    print(f"Conflicts:  {conflicts}")
    print(f"Failed:     {failed}")

    if conflicts or failed:
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

    parser.add_argument(
        "--overwrite",
        action="store_true",
        help=(
            "Replace an existing object when its checksum differs "
            "or it has not stored checksum"
        )
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
            overwrite=args.overwrite
        )
    except FileNotFoundError as error:
        print(f"File error: {error}")
        raise SystemExit(1)
    except NoCredentialsError:
        print("AWS credentials were not found. Run 'aws configure'.")
        raise SystemExit(1)


if __name__ == "__main__":
    main()