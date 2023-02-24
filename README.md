# airflow-example
Quick example about how implement a simple ETL process with Airflow.

This project has A Dag file called ETL_example which builds the process in airflow by calling three Python functions. These functions are cuspide_scrape, cuspide_process and cuspide_load. Each one runs one step of the process.
The objetive of this project is to show how to organize and schedulize the ETL process in order to populate a SQL database
