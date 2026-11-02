FROM apache/airflow:3.3.2-python3.12
COPY requirements.txt /tmp/steam-requirements.txt
RUN pip install --no-cache-dir "apache-airflow==3.3.2" -r /tmp/steam-requirements.txt
WORKDIR /opt/airflow/project
