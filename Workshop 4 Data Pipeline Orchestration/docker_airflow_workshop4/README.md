## Task Flow API

เป็นการเขียน DAG แบบ Task Flow API

การรัน 
echo -e "AIRFLOW_UID=$(id -u)" > .env

docker-compose up airflow-init
docker-compose up
docker build -t apache/airflow:2.10.3 .
