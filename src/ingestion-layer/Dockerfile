FROM apache/spark:3.5.7-java17-python3

USER root
ENV HOME=/root
RUN mkdir -p $HOME

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY streaming.py /app/streaming.py