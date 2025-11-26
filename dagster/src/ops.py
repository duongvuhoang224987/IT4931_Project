import docker
from dagster import op, Out, Failure

@op(out=Out())
def run_star_schema_transform(context):
    """
    Submit the star schema transformation Spark job via the Spark Master container.
    """
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
        
        return "Star schema transformation completed successfully"
        
    except docker.errors.NotFound:
        context.log.error("sparkMaster container not found")
        raise Failure("sparkMaster container not found")
    except Exception as e:
        context.log.error(f"Spark job failed: {str(e)}")
        raise Failure(f"Spark job failed: {str(e)}")