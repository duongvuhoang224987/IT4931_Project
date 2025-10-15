FROM apache/spark:4.0.0

USER root

RUN mkdir -p /opt/spark/.ivy2/cache \
    && chown -R 185:0 /opt/spark/.ivy2 \
    && chmod -R u+rw /opt/spark/.ivy2

ENV SPARK_HOME=/opt/spark
ENV PATH="$PATH:/opt/spark/bin"
ENV IVY_HOME=/opt/spark/.ivy2

WORKDIR /opt/spark/
COPY ../requirements.txt .

RUN apt-get update \
    && pip install --no-cache-dir -r requirements.txt

USER spark