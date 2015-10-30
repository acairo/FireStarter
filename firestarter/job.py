"FireStarter": {
    "spark_conf": {
      "num_executors:": 4,
      "app_name": "Fill Your Mother"
    },
    "Reader": {
      "type": "hive_query" {
        "source_db": "polaris",
        "source_table": None,
        "query": queries.query_server_utilization,
        "temp_table": "source"
        }
    },
    "Writer": {
      "type": "hive"{
        "database_name": "acairo",
        "table_name": "server_utilization",
        "query": queries.insert_server_utilization
    }
  }
}
