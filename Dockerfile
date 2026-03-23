FROM apache/airflow:2.9.3

USER airflow
COPY requirements.txt .
RUN cat requirements.txt
RUN pip install --upgrade pip
RUN pip install -r requirements.txt