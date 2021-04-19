### BASEBALL_DB

**PURPOSE:** Maintain a local postgres database with the baseball statistics made available in the `pybaseball` package


**DETAILS:**

For each of the main datasources in the `pybaseball` package, there is a `pull_{datasource}.py` file. Depending on the format of the data, either the MultiYearDataPull class, StatcastDataPull class, or the pull_single_table function are used to pull the data and maintain a corresponding gzip tsv file in the `data/{datasource}/` directory. 

The `update_db.py` script reads the tables that are configured in the `tables.yaml` file and updates the corresponding tables in the postgres database. 


The `pyproject.toml` file includes all environment information, and a list of the packages needed are listed below. This packages was developed using python3.8.

```
pandas = "^1.2.3"
pybaseball = "^2.1.1"
psycopg2-binary = "^2.8.6"
PyYAML = "^5.4.1"
```