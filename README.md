
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

### Prerequisites

1. Install Docker and Docker Compose following the instructions in the [official documentation](https://docs.docker.com/compose/install/).

1. Install Meltano following the instructions in the [official documentation](https://docs.meltano.com/getting-started/installation).

### Setup Source and Target Database

1. Run `docker compose up -d` to set up the source PostgreSQL Northwind database using the provided `docker-compose.yml` file.

1. Ensure you have a PostgreSQL database set up for the target.

Create a `.env` file in the project directory and configure the source and target PostgreSQL connection settings:

``` bash
export PG_HOST=localhost
export PG_PORT=5432
export PG_USER=northwind_user
export PG_PASSWORD=thewindisblowing
export PG_DATABASE=northwind

export TARGET_PG_HOST=localhost
export TARGET_PG_PORT=target_port
export TARGET_PG_USER=target_user
export TARGET_PG_PASSWORD=target_password
export TARGET_PG_DATABASE=target_db
```

Replace `target_port` `target_user`, `target_password`, and `target_db` with an available port, your desired credentials and database name.

Load the environment variables: `source .env.`

Then, execute:

``` bash
docker run -d --name analytics-db -p "$TARGET_PG_PORT":5432 -e POSTGRES_USER="$TARGET_PG_USER" -e POSTGRES_PASSWORD="$TARGET_PG_PASSWORD" -e POSTGRES_DB="$TARGET_PG_DATABASE" postgres:latest
```

### Create Meltano Project

1. Create a new Meltano project using `meltano init indicium-code-challenge`.

1. Change to the project directory: `cd indicium-code-challenge`.

## Configure Source and Target Database Connections

1. Add the source PostgreSQL connector: `meltano add extractor tap-postgres`.

1. Add the target PostgreSQL connector: `meltano add loader target-postgres`.

### Configure CSV Source

1. Add the CSV extractor: `meltano add extractor tap-csv`.

1. Create a `csv_config.yaml` file in the project directory with the following content:

``` yaml
file: "data/order_details.csv"
delimiter: ","
key_properties:
  - order_id
```

Add the configuration file to Meltano: `meltano config tap-csv set --file csv_config.yaml`.

### Add and Configure Target CSV Loader

1. Add the target CSV loader: `meltano add loader target-csv`.

1. Create a `csv_config.yaml` file in the project directory with the following content:

``` yaml
delimiter: ","
quotechar: '"'
destination_path: "data/"

    Add the configuration file to Meltano: meltano config target-csv set --file csv_config.yaml.
```

Update Meltano Configuration

Update the meltano.yml file in the project directory to include the target CSV loader:

``` yaml
plugins:
  extractors:
    - name: tap-postgres
      variant: singer-io
      namespace: meltano
      config:
        host_env: PG_HOST
        port_env: PG_PORT
        user_env: PG_USER
        password_env: PG_PASSWORD
        dbname_env: PG_DATABASE
    - name: tap-csv
      variant: singer-io
      namespace: meltano
      config:
        key_properties:
          - order_id
  loaders:
    - name: target-csv
      variant: singer-io
      namespace: meltano
      config:
        delimiter: ","
        quotechar: '"'
        destination_path: "data/"
```

### Run the Data Pipeline to Save Data to Local File System

1. Run the data pipeline for PostgreSQL tables to save data to the local file system: `meltano elt tap-postgres target-csv --job_id=postgres-lfs-EL`.

1. Run the data pipeline for the CSV file to save data to the local file system: `meltano elt tap-csv target-csv --job_id=csv-lfs-EL`.

At this point, the extracted data will be saved as CSV files in the `data/` directory. Each file will be stored in a folder named after the table, and each folder will have a date-based folder structure.

### Load Data from Local File System to Target PostgreSQL Database

To load data from the local file system to the target PostgreSQL database using Meltano, we can create a custom loader plugin. This custom loader will read the CSV files generated by the target-csv loader and insert the data into the target PostgreSQL database.

#### Create a Custom Meltano Loader

1. Create a new directory called `custom_loader` in your Meltano project directory.

1. Inside the `custom_loader` directory, create a `meltano.yml` file with the following content:

``` yaml
name: custom-loader
namespace: custom
entrypoint: custom_loader.loader:CustomLoader
```

Create a `loader.py` file inside the `custom_loader` directory with the following content:

``` python
import csv
import os
import psycopg2
from meltano.core.loader import Loader
from meltano.core.utils import truthy
from meltano.core.config_service import ConfigService


class CustomLoader(Loader):
    def load(self, context):
        # Connect to the target PostgreSQL database
        conn = psycopg2.connect(
            host=os.getenv("TARGET_PG_HOST"),
            port=os.getenv("TARGET_PG_PORT"),
            user=os.getenv("TARGET_PG_USER"),
            password=os.getenv("TARGET_PG_PASSWORD"),
            dbname=os.getenv("TARGET_PG_DATABASE"),
        )

        # Load CSV files from the local file system
        source_directory = "data/"
        for root, dirs, files in os.walk(source_directory):
            for file in files:
                if file.endswith(".csv"):
                    table_name = os.path.splitext(file)[0]
                    file_path = os.path.join(root, file)

                    with open(file_path, mode="r") as csv_file:
                        csv_reader = csv.DictReader(csv_file)
                        columns = csv_reader.fieldnames

                        # Create a table in the target PostgreSQL database
                        with conn.cursor() as cursor:
                            create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join([f'{column} TEXT' for column in columns])})"
                            cursor.execute(create_table_sql)
                            conn.commit()

                        # Load data from the CSV file into the target PostgreSQL database
                        with conn.cursor() as cursor:
                            for row in csv_reader:
                                insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s' for _ in columns])})"
                                cursor.execute(insert_sql, list(row.values()))
                            conn.commit()

        # Close the connection to the target PostgreSQL database
        conn.close()
```

In your Meltano project directory, run `meltano discover all` to discover the custom loader plugin.

Add the custom loader plugin to your Meltano project: `meltano add loader custom-loader`.

### Configure the Data Pipeline

Create a `meltano.yml` file in the project directory, which will define the data pipeline with the extractors and loaders added earlier.

``` yaml
plugins:
  extractors:
    - name: tap-postgres
      variant: singer-io
      namespace: meltano
      config:
        host_env: PG_HOST
        port_env: PG_PORT
        user_env: PG_USER
        password_env: PG_PASSWORD
        dbname_env: PG_DATABASE
    - name: tap-csv
      variant: singer-io
      namespace: meltano
      config:
        key_properties:
          - order_id
  loaders:
    - name: target-csv
      variant: singer-io
      namespace: meltano
      config:
        delimiter: ","
        quotechar: '"'
        destination_path: "data/"
    - name: target-postgres
      variant: singer-io
      namespace: meltano
      config:
        host_env: TARGET_PG_HOST
        port_env: TARGET_PG_PORT
        user_env: TARGET_PG_USER
        password_env: TARGET_PG_PASSWORD
        dbname_env: TARGET_PG_DATABASE
    - name: custom-loader
      namespace: custom
      config: {}

```

Now you can use the custom loader to load data from the local file system to the target PostgreSQL database: `meltano elt custom-loader --job_id=lfs-analytics-EL`.

The custom loader will read the locally saved CSV files generated by the `target-csv` loader and insert the data into the target PostgreSQL database.

### Query the Final Database

After running the data pipeline, you can query the analytics database and export the result as a `.csv` as follows:

1. Connect to the analytics database: `docker exec -it analytics-db psql -U target_user`

1. Once you are connected, you can run a query to select the data you want to export to a CSV file. For example, a query that satisfies the challenge requirement is:

``` sql
\copy (
    SELECT * 
    FROM orders AS o
    JOIN order_details AS od
    ON o.ORDER_ID = od.ORDER_ID
) to 'data/query-results.csv' with csv header
```

### Schedule the Pipeline with Meltano

Schedule the data pipeline for the PostgreSQL tables:

``` bash
meltano schedule postgres_pipeline tap-postgres target-csv custom-loader @daily --transform=run
```

Schedule the data pipeline for the CSV file:

``` bash
meltano schedule csv_pipeline tap-csv target-csv custom-loader @daily --transform=run
```

Ensure the Meltano scheduler is running. You can start the scheduler with the following command:

``` bash
meltano schedule start
```

This command will start the Meltano scheduler, which will run the scheduled pipelines at the specified intervals. In this case, both the `postgres_pipeline` and `csv_pipeline` will run daily.

To check the status of the scheduled pipelines, you can use the following command:

``` bash
meltano schedule list
```

This command will display a list of all scheduled pipelines in your Meltano project and their current status.

- Repo owner or admin
- Other community or team contact
