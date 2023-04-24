
# Indicium Tech Code Challenge

Code challenge for Software Developer with focus in data projects.

## Context

At Indicium we have many projects where we develop the whole data pipeline for our client, from extracting data from many data sources to loading this data at its final destination, with this final destination varying from a data warehouse for a Business Intelligency tool to an api for integrating with third party systems.

As a software developer with focus in data projects your mission is to plan, develop, deploy, and maintain a data pipeline.

## The Challenge

We are going to provide 2 data sources, a Postgres database and a CSV file.

The CSV file represents details of orders from a ecommerce system.

The database provided is a sample database provided by microsoft for education purposes called northwind, the only difference is that the order_detail table does not exists in this database you are beeing provided with.This order_details table is represented by the CSV file we provide.

Schema of the original Northwind Database:

![image](https://user-images.githubusercontent.com/49417424/105997621-9666b980-608a-11eb-86fd-db6b44ece02a.png)

Your mission is to build a pipeline that extracts the data everyday from both sources and write the data first to local disk, and second to a database of your choice. For this challenge, the CSV file and the database will be static, but in any real world project, both data sources would be changing constantly.

Its important that all writing steps are isolated from each other, you shoud be able to run any step without executing the others.

For the first step, where you write data to local disk, you should write one file for each table and one file for the input CSV file. This pipeline will run everyday, so there should be a separation in the file paths you will create for each source(CSV or Postgres), table and execution day combination, e.g.:

``` bash
/data/postgres/{table}/2021-01-01/file.format
/data/postgres/{table}/2021-01-02/file.format
/data/csv/2021-01-02/file.format
```

you are free to chose the naming and the format of the file you are going to save.

At step 2, you should load the data from the local filesystem to the final database that you chosed.

The final goal is to be able to run a query that shows the orders and its details. The Orders are placed in a table called **orders** at the postgres Northwind database. The details are placed at the csv file provided, and each line has an **order_id** field pointing the **orders** table.

How you are going to build this query will heavily depend on which database you choose and how you will load the data this database.

The pipeline will look something like this:

![image](https://user-images.githubusercontent.com/49417424/105993225-e2aefb00-6084-11eb-96af-3ec3716b151a.png)

## Requirements

- All tasks should be idempotent, you should be able the whole pipeline for a day and the result should be always the same
- Step 2 depends on both tasks of step 1, so you should not be able to run step 2 for a day if the tasks from step 1 did not succeed
- You should extract all the tables from the source database, it does not matter that you will not use most of them for the final step.
- You should be able to tell where the pipeline failed clearly, so you know from which step you should rerun the pipeline
- You have to provide clear instructions on how to run the whole pipeline. The easier the better.
- You have to provide a csv or json file with the result of the final query at the final database.
- You dont have to actually schedule the pipeline, but you should assume that it will run for different days.
- Your pipeline should be prepared to run for past days, meaning you should be able to pass an argument to the pipeline with a day from the past, and it should reprocess the data for that day. Since the data for this challenge is static, the only difference for each day of execution will be the output paths.

## Things that Matters

- Clean and organized code.
- Good decisions at which step (which database, which file format..) and good arguments to back those decisions up.

## Setup of the source database

The source database can be set up using docker compose.
You can install following the instructions at
<https://docs.docker.com/compose/install/>

With docker compose installed simply run

``` bash
docker compose up
```

You can find the credentials at the docker-compose.yml file

## Final Instruction

You can use any language you like, but keep in mind that we will have to run your pipeline, so choosing some languague or tooling that requires a complex environment might not be a good idea.
You are free to use opensource libs and frameworks, but also keep in mind that **you have to write code**. Point and click tools are not allowed.

Thank you for participating!

## Solution

To address the Indicium Tech Code Challenge, we will use Meltano, a powerful open-source data pipeline tool. Meltano simplifies the process of extracting, transforming, and loading data from various sources, making it a perfect fit for this challenge.

### Setup

Create a `.env` file in the project directory and configure environment variables:

``` bash
TAP_POSTGRES_HOST=db
TAP_POSTGRES_PORT=5432
TAP_POSTGRES_USER=northwind_user
TAP_POSTGRES_PASSWORD=thewindisblowing
TAP_POSTGRES_DBNAME=northwind
TAP_POSTGRES_DEFAULT_REPLICATION_METHOD=INCREMENTAL

TARGET_POSTGRES_HOST=analytics-db
TARGET_POSTGRES_PORT=5432
TARGET_POSTGRES_USER=target_user
TARGET_POSTGRES_PASSWORD=target_password
TARGET_POSTGRES_DBNAME=target_db
TARGET_POSTGRES_DEFAULT_TARGET_SCHEMA=public

MELTANO_ENVIRONMENT=dev
```

Replace `target_port` `target_user`, `target_password`, and `target_db` with an available port, your desired credentials and database name.

Run `source .env` and `docker compose up -d`. This will setup a `db` service correspondent to the Northwind database, an `analytics-db` service correpondent to the target Postgres database, and a `server` service which contains meltano and all necessary plugins for our project.

To make each table in the Northwind db set up for incremental extraction, rum `docker exec db psql -U northwind_user -d northwind -f /home/scripts/set-incremental-public.sql`.

### Query the Final Database

After running the data pipeline, you can query the analytics database and export the result as a `.csv` as follows:

1. Connect to the analytics database: `docker exec -it analytics-db psql -U target_user -d target_db`

1. Once you are connected, you can run a query to select the data you want to export to a CSV file. For example, a query that satisfies the challenge requirement is:

``` sql
\copy (
    SELECT * 
    FROM orders AS o
    JOIN order_details AS od
    ON o.ORDER_ID = od.ORDER_ID
) to '/home/data/query-results.csv' with csv header
```

### Schedule the Pipelines with Meltano

1. Run the scheduled pipes: `docker exec server meltano invoke airflow scheduler -D`.

### Executing specific tasks

Available tasks:

- meltano_csv-to-lfs
- meltano_postgres-to-lfs
- meltano_lfs-to-analytics

1. Run specific task for today: `docker exec server invoke airflow dags trigger TASK`.

1. Run specific task for past date: `docker exec server invoke airflow backfill -s DATE -e DATE TASK`.
