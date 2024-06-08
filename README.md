# DEMA.AI Data Ingestion Pipeline

## Objective:
Build a data pipeline to ingest the data from a given location and process them into a database. The two datasets are orders and inventory.

After ingestion of raw data, it is transformed into a merged table suitable for analytics queries.

## Assumptions made:
1. There will be only 2 csv files which will be ingested.
2. The pipeline will be scheduled in every 2 hours.
3. In case of failure it will retry with a delay of 5 mins.

## Tech Stack:
* Docker
* Python
* Airflow
* Postgres

## Environment Setup:
1. Clone the repository:
    ```
    git clone https://github.com/Priya7369/DemaAI
    cd DemaAI/
    ```

2. Run the below commands:
   
   Run airflow in docker
   ```
   docker-compose up airflow-init
   ```

   To start the docker
   ```
   docker-compose up
   ```

   To stop the docker
   ```
   docker-compose down
   ```

3. Now open the airflow UI at http://localhost:8080/
username: airflow
password: airflow

4. The DAG named "ecommerce_data_pipelines" will be running and the log files will be generated in the folder logs/ 


## Possible Improvements (if given more time):
1. Extensive data validation checks during ingestion and transformation stages
2. Improve DAG's code (readability)
3. Improve error handling in each stage.
4. Track data quality metrics and set up alerts for the same.
5. Understand the raw data in detail.
6. Develop unit test tasks.