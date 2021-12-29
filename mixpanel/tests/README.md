# Data Pipeline
## Mixpanel Export

### Intro
This sub-project is used to Export mixpanel data.
The export job is an Apache Airflow DAG, supported by a custom Airflow Plugin 
that executes a JQL query against Mixpanel's database.

As currently implemented, the job is running on Cloud Composer, 
a Google Cloud product. Therefore, most of the support 
(provided in the Makefile) to update and deploy this project is based on 
running on Google's infrastructure. 

The project is setup to breakup the day into disjoint time intervals, for which
records are retrieved and then inserted into BigQuery.

Mixpanel records have *TWO* important timestamps:  the record event timee, and 
the time mixpanel processed the record. Events in Mixpanel can be delayed up to
*5* days, so we are forced to define our processing intervals on the 
Mixpanel processing time.

**NOTE**: Both event time and processing time are recorded in millisecond 
timestamps, but event time is recorded in *EST* while processing time is *UTC*

#### Components:

1. DAG and dependencies
   1. Airflow DAGS are essentially python code that describe a series of tasks to be run, and in what order
1. Plugins:
    A python package (or module) that performs a specific task or closely 
    related grouping of tasks.
    
### Local Installation for development/testing
1. Create a Python 3.6.6 virtual environment
1. Install the requirements:
```bash
make install
``` 

**Note**:  The way Airflow dynamically loads plugins means any IDE import checking 
will flag custom plugin imports as errors.

When starting Airflow, an environemnt variable (or config setting) declares a
plugin directory, which airflow will scan and load any custom plugins.  This is
handled in all the *Makefile* directives. 


### Testing
This project has several levels of testing:
* Linting: Using the standard ACV pylintrc;
* Smoke tests: start the python interpreter and see if the source compiles;
* Unit tests: Provided for the plugin;
* Functional Tests: Start Airflow and load the plugins and DAGS. 

THe functional test not only loads the dag and plugin, but also renders any 
templates in the dependencies folder.

These tests can be run by the following directives:
```bash
make lint # linting
make test # run smoke, unit, functional tests
make smoke-test
make unit-test
make functional-test
```


### Local installations
Due to the DAG accessing Google Cloud Products, the DAG may fail on local 
installations because of authentication issues.  
That said, the directive `make local-update` will copy the *dags* and *plugins*
to the **$AIRFLOW_HOME** directory 
(defaulting to **~/airflow/dags** and **~/airflow/plugins**, respectively).

### Deploying to Production
1. User should authenticate against google's systsems with `gcloud auth login`
1. I provide several make directives to handle production deployments: 
    1. one to push dags `make deploy-dag`;
    1. one to update plugins `make deploy-plugin`;
    1. and one for dependencies `make deploy-dependencies`. 
1. `make deploy` is provided to run all 3 deploy commands.

1. The Airflow scheduler will automatically pickup the updates, and execute the 
dag according to the provided start_date and run interval. 

**NOTE**: it may take several minutes for the scheduler to pickup DAG changes.
Any changes to the plugin should be in effect immediately.


### Monitoring and Alerting
This project uses Stackdriver log-based and custom metrics to define states 
which we wish to alert. Some of the alerts are:
* Mixpanel calls returning http-500, 3 or more in 5 minutes
* The absence of operators reporting how many records were retrieved (if no 
records are retrieved, the operator logs a 'retrieved: 0 records' message)
* GCE resource utilization (cpu, memory)

The Alerts are pushed to PagerDuty, which in turn forward to Slack and email.

See [Stackdriver Monitoring](https://app.google.stackdriver.com/)


### In the Future:
* local Airflow Docker containers



## Notes

### Variables:

All variables for an Airflow job should be contained in a JSON dictionary, and 
stored in a single Variable object in the Airflow Database.
( Each call to retrieve a variable in a DAG file opens, then closes a DB connection.  This opening and closing
happens EVERY TIME a dag is processed by the scheduler.  This consumes quite a bit of resources)

```json
{
    "mixpanel-export": {
        "gcs_bucket": "",
        "gcp_dataset": "",
        "gcp_project": "",
        "bigquery_dataset": "",
        "bigquery_event_table": "events",
        "bigquery_people_table": "people"
    }
}
```


#### export
##### From airflow to container fs 
Takes composer environment variables and exports them to a json in the composer (local) filesystem.
(The filesystem directory is attached to GCS)
```bash
gcloud composer environments run export-test2 --location us-east1 variables \
-- --export /home/airflow/gcs/data/variables.json
```
##### Export the GCS/composer filesystem file to local filesystem

```bash
gcloud composer environments storage data export --destination /tmp \
--source variables.json --environment export-test2 --location us-east1
```

#### Import
##### Upload JSON to container fs (bucket)
```bash
gcloud composer environments storage data import --source /tmp/variables.json \
--environment export-test2 --location us-east1
```
##### Import variables into new environment
*NOTE* Check for variable name collisions in new environment
```bash
gcloud composer environments run {{ NEW_ENVIRONMENT }} --location us-east1 variables -- --import /home/airflow/gcs/data/variables.json 
```

### Connections:
##### list all connections
```bash
gcloud composer environments run export-test2 --location us-east1 connections -- —-list
```

##### Add a mixpanel (jql) connection 
```bash
gcloud composer environments run export-test2 --location us-east1 connections \
-- --add --conn_id=mixpanel_export --conn_type=http \
--conn_host=http://data.mixpanel.com/api/2.0/export --conn_login=<api_secret>
```

### Source files
##### Deleting a DAG from Airflow
1. Remove the source file from the bucket
```bash
gsutil rm gs://<airflow_environment_bucket>/dags/<path_to_source>/<filename>.py
```
If you try to use the command 
`gcloud composer environments --project acv-data storage dags delete ...`, 
it will recursively delete & I haven't found the courage to run that command.
2. Remove run information from the Airflow server
```bash
gcloud composer environments \
--project <project> \
run <environment> \
--location <location> \
delete_dag -- [--yes] <dag_name>
```

### Dependencies

```
gs://bucket
+-dags
| |
| +-mixpanel
|   |
|   +-dependencies
+-data
| |
| +-schemas
+-plugins
```


##### Uploading BigQuery Schemas
Go into a GCS Bucket, here we use a sub-folder of data 
```bash
gcloud composer environments storage data import \
--source mixpanel/schemas \
--environment airflow-pipeline \
--location us-east1 \
--project acv-data

```

##### Uploading DAGS (with Templates)
Go into (a subdir of) the mixpanel dags folder
```bash
gcloud composer environments storage dags import \
--source mixpanel/dags/dependencies \
--environment airflow-pipeline \
--location us-east1 \
--destination mixpanel \--project acv-data 
```

[Reference](https://cloud.google.com/sdk/gcloud/reference/composer/environments/storage/dags/import)

