import functions_framework
from googleapiclient.discovery import build

# make sure the in runtime settings, the number
# of max instances is set to the lowest number
# i am not sure whether it was the changing
# jobname along with the default 100 instances
# which triggered several DataFlow jobs which
# eventually resulted in a quota exceded error
# https://medium.com/@aishwarya.gupta3/cloud-function-to-start-a-data-flow-job-on-a-new-file-upload-in-google-cloud-storage-using-trigger-30270b31a06d

# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def start_cf_to_dataflow_to_bq_flow(cloud_storage):
	#replace with your projectID
	project = "hive-project-404608"
	payload = cloud_storage.data
	output_table = f"{project}:healthcare_app.patients"
	date_created = payload['timeCreated'].split("T")[0]
	job = f"health-batch-dataflow-{date_created}"
	# job = f"health-batch-dataflow-{payload['timeCreated']}"
	#path of the dataflow template on google storage bucket
	template = f"gs://{payload['bucket']}/templates/GCS_Text_to_BigQuery"
	input_file = f"gs://{payload['bucket']}/{payload['name']}"
	js_udf = "transform"
	json_path = f"gs://{payload['bucket']}/schema/bq_health_schema.json"
	js_path = f"gs://{payload['bucket']}/transform_script/*.js"
	bq_temp_dir = f"gs://{payload['bucket']}/temp/"
	bq_temp_file = f"gs://{payload['bucket']}/temp/log"
	#user defined parameters to pass to the dataflow pipeline job
	parameters = {
		'inputFilePattern': input_file,
		"javascriptTextTransformFunctionName": js_udf,
		"outputTable": output_table,
		"JSONPath": json_path,
		"javascriptTextTransformGcsPath": js_path,
		"bigQueryLoadingTemporaryDirectory": bq_temp_dir,
	}
	#tempLocation is the path on GCS to store temp files generated during the dataflow job
	environment = {
		'tempLocation': bq_temp_file
	}

	service = build('dataflow', 'v1b3', cache_discovery=False)
	#below API is used when we want to pass the location of the dataflow job
	request = service.projects().locations().templates().launch(
		projectId=project,
		gcsPath=template,
		location='us-central1',
		body={
			'jobName': job,
			'parameters': parameters,
			'environment':environment
		},
	)
	response = request.execute()
	print(response)
