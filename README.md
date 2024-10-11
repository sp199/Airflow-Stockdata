This work ports ETL wrokflow from Google Colab to Cloud Composer (Airflow) with the objective of automating stock data retrieval, processing, and storage using the Alpha Vantage API and Snowflake.

The Work includes several key steps.

First, all required Python modules were imported, and any missing packages were added to Cloud Composer’s PyPI packages section. Using Airflow’s @task decorator, multiple tasks were created for extracting, transforming, and loading stock data, modularizing the workflow for clarity and efficiency.

An Airflow variable was set up for the Alpha Vantage API key, allowing for secure retrieval of the key using Variable.get in the DAG. Screenshots were captured to document the configuration in Admin > Variables. Additionally, a connection to Snowflake was established in Airflow by filling in the necessary credentials such as warehouse, database, and schema. This connection was then used within the DAG to load the data into Snowflake, with the connection details page screenshot for documentation.

To complete the task, the entire DAG was executed successfully, performing the end-to-end data pipeline. Screenshots of the Airflow homepage and the DAG's log screen were captured to verify successful execution.
