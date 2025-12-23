"""
Dagster Assets cho Kubernetes (Minikube)
Thay thế docker.from_env() bằng Kubernetes API
"""
from kubernetes import client, config, stream
from dagster import asset, Failure

# Load Kubernetes config (in-cluster hoặc local kubeconfig)
try:
    config.load_incluster_config()  # Khi chạy trong K8s cluster
except:
    config.load_kube_config()  # Khi chạy local với kubeconfig

v1 = client.CoreV1Api()

@asset
def star_schema_data(context):
    """
    Star schema transformation data in ClickHouse using Spark.
    Kubernetes version - executes in Spark Master pod.
    """
    namespace = "default"

    # Find ClickHouse pod first to verify database
    try:
        pods = v1.list_namespaced_pod(
            namespace=namespace,
            label_selector="app=clickhouse"
        )
        clickhouse_pod = pods.items[0].metadata.name

        check_cmd = [
            "clickhouse-client",
            "--user", "it4931",
            "--password", "it4931",
            "--query", "SHOW DATABASES LIKE 'nyc_taxi_dw'"
        ]

        resp = stream.stream(
            v1.connect_get_namespaced_pod_exec,
            clickhouse_pod,
            namespace,
            command=check_cmd,
            stderr=True,
            stdin=False,
            stdout=True,
            tty=False
        )

        if not resp.strip():
            raise Failure("ClickHouse database 'nyc_taxi_dw' does not exist.")

        context.log.info("ClickHouse database exists, proceeding with transformation")

    except Exception as e:
        raise Failure(f"Failed to verify ClickHouse database: {str(e)}")

    # Find Spark Master pod
    try:
        pods = v1.list_namespaced_pod(
            namespace=namespace,
            label_selector="app=spark-master"  # Adjust label
        )
        if not pods.items:
            raise Failure("No Spark Master pod found")
        spark_pod = pods.items[0].metadata.name
        context.log.info(f"Found Spark Master pod: {spark_pod}")
    except Exception as e:
        raise Failure(f"Failed to find Spark Master pod: {str(e)}")

    spark_job_path = "/opt/spark/src/spark_jobs/star_schema_transform.py"

    spark_cmd = [
        "/opt/spark/bin/spark-submit",
        spark_job_path
    ]

    context.log.info(f"Executing Spark job: {' '.join(spark_cmd)}")

    try:
        resp = stream.stream(
            v1.connect_get_namespaced_pod_exec,
            spark_pod,
            namespace,
            command=spark_cmd,
            stderr=True,
            stdin=False,
            stdout=True,
            tty=False,
            _preload_content=False
        )

        output = ""
        while resp.is_open():
            resp.update(timeout=1)
            if resp.peek_stdout():
                stdout = resp.read_stdout()
                output += stdout
                context.log.info(stdout.rstrip())
            if resp.peek_stderr():
                stderr = resp.read_stderr()
                context.log.error(stderr.rstrip())

        resp.close()

        return {"status": "completed", "job": "star_schema_transform", "pod": spark_pod}

    except Exception as e:
        context.log.error(f"Spark job failed: {str(e)}")
        raise Failure(f"Spark job failed: {str(e)}")


@asset(deps=["star_schema_data"])
def run_batch_job(context):
    """
    Submit the batch job Spark job via the Spark Master pod.
    Kubernetes version.
    """
    namespace = "default"

    # Find Spark Master pod
    try:
        pods = v1.list_namespaced_pod(
            namespace=namespace,
            label_selector="app=spark-master"
        )
        spark_pod = pods.items[0].metadata.name
        context.log.info(f"Found Spark Master pod: {spark_pod}")
    except Exception as e:
        raise Failure(f"Failed to find Spark Master pod: {str(e)}")

    spark_job_path = "/opt/spark/src/spark_jobs/batch_job.py"

    spark_cmd = [
        "/opt/spark/bin/spark-submit",
        # "--master", "spark://spark-master-svc:7077",
        # "--deploy-mode", "client",
        # "--conf", "spark.sql.legacy.timeParserPolicy=LEGACY",
        # "--jars", "/opt/spark/jars/clickhouse-jdbc-shaded.jar",
        spark_job_path
    ]

    context.log.info(f"Executing Spark job: {' '.join(spark_cmd)}")

    try:
        resp = stream.stream(
            v1.connect_get_namespaced_pod_exec,
            spark_pod,
            namespace,
            command=spark_cmd,
            stderr=True,
            stdin=False,
            stdout=True,
            tty=False,
            _preload_content=False
        )

        output = ""
        while resp.is_open():
            resp.update(timeout=1)
            if resp.peek_stdout():
                stdout = resp.read_stdout()
                output += stdout
                context.log.info(stdout.rstrip())
            if resp.peek_stderr():
                stderr = resp.read_stderr()
                context.log.error(stderr.rstrip())

        resp.close()

        return {"status": "completed", "job": "batch_job", "pod": spark_pod}

    except Exception as e:
        context.log.error(f"Spark job failed: {str(e)}")
        raise Failure(f"Spark job failed: {str(e)}")
