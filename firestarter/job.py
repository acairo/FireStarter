"FireStarter": {
    "spark_conf": {
      "app_name": "Fill Your Mother",
      "parameters": {
        "num_executors:": 4,
      }
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
