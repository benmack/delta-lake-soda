from soda.common.json_helper import JsonHelper

def build_scan_results(scan) -> dict:

    checks = [
        check.get_cloud_dict() for check in scan._checks if check.outcome is not None and check.archetype is None
    ]
    autoamted_monitoring_checks = [
        check.get_cloud_dict()
        for check in scan._checks
        if check.outcome is not None and check.archetype is not None
    ]

    # TODO: [SODA-608] separate profile columns and sample tables by aligning with the backend team
    profiling = [
        profile_table.get_cloud_dict()
        for profile_table in scan._profile_columns_result_tables + scan._sample_tables_result_tables
    ]

    return JsonHelper.to_jsonnable(  # type: ignore
        {
            "definitionName": scan._scan_definition_name,
            "defaultDataSource": scan._data_source_name,
            "dataTimestamp": scan._data_timestamp,
            "scanStartTimestamp": scan._scan_start_timestamp,
            "scanEndTimestamp": scan._scan_end_timestamp,
            "hasErrors": scan.has_error_logs(),
            "hasWarnings": scan.has_check_warns(),
            "hasFailures": scan.has_check_fails(),
            "metrics": [metric.get_cloud_dict() for metric in scan._metrics],
            # If archetype is not None, it means that check is automated monitoring
            "checks": checks,
            # TODO Queries are not supported by Soda Cloud yet.
            # "queries": [query.get_cloud_dict() for query in scan._queries],
            "automatedMonitoringChecks": autoamted_monitoring_checks,
            "profiling": profiling,
            "metadata": [
                discover_tables_result.get_cloud_dict()
                for discover_tables_result in scan._discover_tables_result_tables
            ],
            "logs": [log.get_cloud_dict() for log in scan._logs.logs],
        }
    )

def save_scan_results_with_spark_as_json(scan_results, path, spark):
    rdd = spark.sparkContext.parallelize([JsonHelper.to_json(scan_results)])
    rdd.coalesce(1).saveAsTextFile(path)
