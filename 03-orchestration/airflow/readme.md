To create an Apache Airflow 3.0 environment using Docker, you should use the official Airflow Docker Compose setup. Here’s a step-by-step guide:
1. Prerequisites
Docker and Docker Compose installed on your machine.
2. Get the Official Docker Compose YAML
The Airflow project provides a ready-to-use docker-compose.yaml file. For Airflow 3.0, you can use the following:
Apply to .env
Run
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/3.0.0/docker-compose.yaml'
3. Set Airflow Version
Create a .env file in the same directory as your docker-compose.yaml with the following content:
Apply to .env
AIRFLOW_IMAGE_NAME=apache/airflow:3.0.0
4. Initialize the Environment
Run the following commands to set up the environment:
Apply to .env
Run
docker compose up airflow-init
5. Start Airflow
Apply to .env
Run
up
This will start all Airflow services (webserver, scheduler, etc.).
6. Access Airflow
Open your browser and go to: http://localhost:8080
Default credentials:
Username: airflow
Password: airflow
Summary of Steps:
Download the official docker-compose.yaml.
Set AIRFLOW_IMAGE_NAME=apache/airflow:3.0.0 in .env.
Run docker compose up airflow-init.
Run docker compose up.
Access Airflow at http://localhost:8080.
