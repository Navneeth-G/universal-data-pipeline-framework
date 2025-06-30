FROM apache/airflow:2.10.5

USER root

RUN curl -fsSL https://deb.nodesource.com/setup_18.x | bash - && apt-get install -y nodejs && npm install -g elasticdump@6.119.1

USER airflow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
