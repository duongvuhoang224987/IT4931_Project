import docker
from dagster import asset, Failure

@asset
def clickhouse_schema(context):
    """
    Create ClickHouse database and tables for the NYC Taxi data warehouse.
    This asset represents the ClickHouse schema setup.
    """
    sql_file_path = "/etc/clickhouse-server/to_clickhouse.sql"
    
    # Build clickhouse-client command
    # Using --queries-file to execute the SQL file directly
    clickhouse_cmd = [
        "clickhouse-client",
        "--user", "it4931",
        "--password", "it4931",
        "--multiquery",
        "--queries-file", sql_file_path
    ]
    
    context.log.info(f"Setting up ClickHouse schema from {sql_file_path}")
    
    try:
        client = docker.from_env()
        container = client.containers.get('clickhouse')
        
        result = container.exec_run(
            cmd=clickhouse_cmd,
            stream=True
        )
        
        # Stream output
        output = ""
        for chunk in result.output:
            chunk_str = chunk.decode('utf-8')
            output += chunk_str
            if chunk_str.strip():
                context.log.info(chunk_str.rstrip())
        
        context.log.info("ClickHouse schema setup completed successfully")
        return {"status": "completed", "sql_file": sql_file_path}

    except Exception as e:
        context.log.error(f"ClickHouse schema setup failed: {str(e)}")
        raise Failure(f"ClickHouse schema setup failed: {str(e)}")


@asset(deps=["clickhouse_schema"])
def star_schema_data(context):
    """
    Star schema transformation data in ClickHouse.
    This asset represents the transformed data using star schema.
    Depends on: clickhouse_schema (must be created first)
    """

    try:
        client = docker.from_env()
        container = client.containers.get('clickhouse')
        
        check_cmd = [
            "clickhouse-client",
            "--user", "it4931",
            "--password", "it4931",
            "--query", "SHOW DATABASES LIKE 'nyc_taxi_dw'"
        ]
        
        result = container.exec_run(cmd=check_cmd)
        if not result.output.strip():
            raise Failure("ClickHouse database 'nyc_taxi_dw' does not exist. Run create_star_schema_job first.")
        
        context.log.info("ClickHouse database exists, proceeding with transformation")
        
    except Exception as e:
        raise Failure(f"Failed to verify ClickHouse database: {str(e)}")
    
    spark_job_path = "/opt/spark/src/spark_jobs/star_schema_transform.py"
    
    # Build spark-submit command
    spark_cmd = [
        "/opt/spark/bin/spark-submit",
        "--master", "spark://sparkMaster:7077",
        "--deploy-mode", "client",
        "--conf", "spark.sql.legacy.timeParserPolicy=LEGACY",
        "--jars", "/opt/spark/jars/clickhouse-jdbc-shaded.jar",
        spark_job_path
    ]
    
    context.log.info(f"Executing Spark job in sparkMaster container: {' '.join(spark_cmd)}")
    
    try:
        client = docker.from_env()
        container = client.containers.get('sparkMaster')
        
        result = container.exec_run(
            cmd=spark_cmd,
            stream=True
        )
        
        # Stream output
        output = ""
        for chunk in result.output:
            chunk_str = chunk.decode('utf-8')
            output += chunk_str
            context.log.info(chunk_str.rstrip())
        
        return {"status": "completed", "job": "star_schema_transform"}
    
    except Exception as e:
        context.log.error(f"Spark job failed: {str(e)}")
        raise Failure(f"Spark job failed: {str(e)}")
    
    
@asset(deps=["star_schema_data"])
def run_batch_job(context):
    """
    Submit the batch job Spark job via the Spark Master container.
    """
    spark_job_path = "/opt/spark/src/spark_jobs/batch_job.py"
    
    # Build spark-submit command
    spark_cmd = [
        "/opt/spark/bin/spark-submit",
        "--master", "spark://sparkMaster:7077",
        "--deploy-mode", "client",
        "--conf", "spark.sql.legacy.timeParserPolicy=LEGACY",
        "--jars", "/opt/spark/jars/clickhouse-jdbc-shaded.jar",
        spark_job_path
    ]
    
    context.log.info(f"Executing Spark job in sparkMaster container: {' '.join(spark_cmd)}")
    
    try:
        client = docker.from_env()
        container = client.containers.get('sparkMaster')
        
        result = container.exec_run(
            cmd=spark_cmd,
            stream=True
        )
        
        # Stream output
        output = ""
        for chunk in result.output:
            chunk_str = chunk.decode('utf-8')
            output += chunk_str
            context.log.info(chunk_str.rstrip())
        return "Batch job completed successfully"
    
    except Exception as e:
        context.log.error(f"Spark job failed: {str(e)}")
        raise Failure(f"Spark job failed: {str(e)}")