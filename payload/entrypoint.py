import time

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lower, trim, upper

from datacustomcode.client import Client
from datacustomcode.io.writer.base import WriteMode

SOURCE_DLO = "airline_large_50000csv__dll"
TARGET_DLO = "airline_large_50000_copy__dll"

# System / key-qualifier columns that Data 360 generates itself
COLUMNS_TO_DROP = [
    "kq_customer_id__c",
    "internalorganization__c",
    "datasource__c",
    "datasourceobject__c",
    "cdp_sys_sourceversion__c",
]

EMAIL_REGEX = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"


def split_clean_and_error(df: DataFrame):
    """Rows with a missing customer id or an invalid email are 'error' rows."""
    has_id = col("customer_id__c").isNotNull() & (trim(col("customer_id__c")) != "")
    has_email = col("email__c").isNotNull() & trim(col("email__c")).rlike(EMAIL_REGEX)
    is_valid = has_id & has_email
    return df.filter(is_valid), df.filter(~is_valid)


def transform(df: DataFrame) -> DataFrame:
    df = df.withColumn("country__c", upper(trim(col("country__c"))))
    df = df.withColumn("email__c", lower(trim(col("email__c"))))
    to_drop = [c for c in COLUMNS_TO_DROP if c in df.columns]
    return df.drop(*to_drop)


def main() -> None:
    start = time.time()
    client = Client()
    try:
        df = client.read_dlo(SOURCE_DLO).cache()
        total = df.count()
        print(f"[INFO] Starting batch transform: {SOURCE_DLO} -> {TARGET_DLO}, rows={total}")

        clean_df, error_df = split_clean_and_error(df)
        clean_count = clean_df.count()
        error_count = error_df.count()

        result = transform(clean_df)
        client.write_to_dlo(TARGET_DLO, result, write_mode=WriteMode.OVERWRITE)

        elapsed = time.time() - start
        print(f"[INFO] Complete: clean={clean_count}, errors={error_count}, elapsed={elapsed:.2f}s")
    except Exception as exc:
        print(f"[ERROR] Batch transform failed: {exc}")
        raise


if __name__ == "__main__":
    main()