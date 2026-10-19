from entrypoint import split_clean_and_error, transform


def _df(spark):
    return spark.createDataFrame(
        [
            ("C1", "  John.Doe@Example.com ", "india", "k1", "src"),
            ("C2", "not-an-email", "usa", "k2", "src"),
            ("C3", None, "uk", "k3", "src"),
            ("C4", "a@b.com", "fr", "k4", "src"),
            (None, "x@y.com", "de", "k5", "src"),
        ],
        ["customer_id__c", "email__c", "country__c", "kq_customer_id__c", "datasource__c"],
    )


def test_email_trimmed_and_lowercased(spark):
    rows = transform(_df(spark)).filter("customer_id__c = 'C1'").collect()
    assert rows[0]["email__c"] == "john.doe@example.com"


def test_country_uppercased(spark):
    rows = transform(_df(spark)).filter("customer_id__c = 'C1'").collect()
    assert rows[0]["country__c"] == "INDIA"


def test_system_columns_dropped(spark):
    out = transform(_df(spark))
    assert "kq_customer_id__c" not in out.columns
    assert "datasource__c" not in out.columns


def test_drop_is_safe_when_columns_missing(spark):
    df = spark.createDataFrame([("C1", "a@b.com", "in")],
                               ["customer_id__c", "email__c", "country__c"])
    assert transform(df).columns == ["customer_id__c", "email__c", "country__c"]


def test_invalid_rows_are_errors(spark):
    clean, err = split_clean_and_error(_df(spark))
    assert clean.count() == 2   # C1 and C4
    assert err.count() == 3     # bad email, null email, null customer id