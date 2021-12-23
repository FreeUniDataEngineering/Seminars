FROM apache/airflow:2.2.2

USER airflow

WORKDIR /app

COPY requirements.txt /app
RUN pip install --trusted-host pypi.python.org -r requirements.txt --user