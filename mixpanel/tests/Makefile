.PHONY: echo install init
.PHONY: update-dependencies update-plugin update-dag update local-update
.PHONY: lint smoke-test unit-test functional-test test clean remote-dag-test
.PHONY: pull-variables get-variables export-variables
.PHONY: push-variables set-variables import-variables


# ============================================================================
# Globals
# ============================================================================
DAG := mixpanel-export
PROJECT := acv-data
ENVIRONMENT := airflow-pipeline

LOCATION := us-east1

echo:
	echo "Deploying to $(PROJECT).$(ENVIRONMENT) for $(DAG)"

install:
	pip install -r requirements.txt

clean:
	find . -type f -name '.DS_Store' -delete -o -type f -name '*.py[co]' -delete -o -type d -name __pycache__ -delete

init:
	airflow initdb

deploy-schemas: echo
	# Schema files are assumed to be in the data bucket
	gcloud composer environments storage data import \
	--source mixpanel/schemas \
	--environment $(ENVIRONMENT) \
	--location $(LOCATION) \
	--project $(PROJECT)


deploy-plugin: echo
	gcloud composer environments storage plugins import \
	--environment $(ENVIRONMENT) \
	--location $(LOCATION) \
	--source mixpanel/plugins/mixpanel_plugin.py \
	--project $(PROJECT)

deploy-dags: echo
	#    grep -IlR --exclude-dir=templates/ "DAG(" ./mixpanel/dags | \
	#    xargs gcloud composer environments storage dags import --environment export-test2 --location $(LOCATION) --source
	gcloud composer environments storage dags import \
	--environment $(ENVIRONMENT) \
	--location $(LOCATION) \
	--source mixpanel/dags \
	--destination mixpanel \
	--project $(PROJECT)

deploy: clean import-variables deploy-schemas deploy-plugin deploy-dags

local-update: init
	AIRFLOW_HOME=$${AIRFLOW_HOME:=~/airflow}
	cp -r mixpanel/dags $(AIRFLOW_HOME)
	cp -r mixpanel/plugins $(AIRFLOW_HOME)

lint:
	pylint --rcfile=.pylintrc mixpanel

smoke-test:
	sh scripts/smoke_test.sh

unit-test:
	sh scripts/run_unit_tests.sh

functional-test:
	sh scripts/run_functional_tests.sh

test: smoke-test unit-test functional-test clean

remote-dag-test:
	# Need to have templates & schemas & plugins in environment
# Upload dag to not the dag folder.
	gcloud composer environments storage data import \
	--environment $(ENVIRONMENT) \
	--location $(LOCATION) \
	--source mixpanel/dags/ \
	--destination test/mixpanel \
	--project $(PROJECT)
# Syntax error check
	gcloud composer environments run \
	--project $(PROJECT) \
	$(ENVIRONMENT) \
	--location $(LOCATION) \
	list_dags -- -sd /home/airflow/gcs/data/test/mixpanel
# Check for task errors
	# extract events
	gcloud composer environments run \
	--project $(PROJECT) \
	$(ENVIRONMENT) \
	--location $(LOCATION) \
	render -- -sd /home/airflow/gcs/data/test/mixpanel \
	mixpanel-export extract_events 2019-01-01T
	# load events
	gcloud composer environments run \
	--project $(PROJECT) \
	$(ENVIRONMENT) \
	--location $(LOCATION) \
	render -- -sd /home/airflow/gcs/data/test/mixpanel \
	mixpanel-export extract_people 2019-01-01
	# clean events
	gcloud composer environments run \
	--project $(PROJECT) \
	$(ENVIRONMENT) \
	--location $(LOCATION) \
	render -- -sd /home/airflow/gcs/data/test/mixpanel \
	mixpanel-export clean_event_objects 2019-01-01
	# extract people
	gcloud composer environments run \
	--project $(PROJECT) \
	$(ENVIRONMENT) \
	--location $(LOCATION) \
	render -- -sd /home/airflow/gcs/data/test/mixpanel \
	mixpanel-export extract_people 2019-01-01
	# load people
	gcloud composer environments run \
	--project $(PROJECT) \
	$(ENVIRONMENT) \
	--location $(LOCATION) \
	render -- -sd /home/airflow/gcs/data/test/mixpanel \
	mixpanel-export extract_people 2019-01-01
	# clean people
	gcloud composer environments run \
	--project $(PROJECT) \
	$(ENVIRONMENT) \
	--location $(LOCATION) \
	render -- -sd /home/airflow/gcs/data/test/mixpanel \
	mixpanel-export clean_people_objects 2019-01-01

pull-variables: echo
	gcloud composer environments run \
	--project $(PROJECT) \
	$(ENVIRONMENT) \
	--location $(LOCATION) \
	variables -- --export /home/airflow/gcs/data/variables.json

get-variables: echo
	gcloud composer environments storage data export \
	--destination /tmp \
	--source variables.json \
	--environment $(ENVIRONMENT) \
	--location $(LOCATION) \
	--project $(PROJECT)

export-variables: echo pull-variables get-variables

push-variables: echo
	echo "Pushing $(SOURCE)"
	gcloud composer environments storage data import \
	--source mixpanel/resources/variables.json \
	--environment $(ENVIRONMENT) \
	--location $(LOCATION) \
	--project $(PROJECT)

set-variables: echo
	gcloud composer environments run \
	--project $(PROJECT) \
	$(ENVIRONMENT) \
	--location $(LOCATION) \
	variables -- --import /home/airflow/gcs/data/variables.json


import-variables: echo push-variables set-variables
