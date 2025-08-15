#Python script to start AWS Glue Job
#Install boto3 using following
#pip install boto3


import boto3

# Create a Glue client
glue = boto3.client('glue', region_name='us-east-1')  # change region if needed

# Trigger the Glue job
response = glue.start_job_run(JobName='your-glue-job-name')

# Print the job run ID
print("Started Glue job run:", response['JobRunId'])


#Check the status of Glue job
job_run_id = response['JobRunId']

status_response = glue.get_job_run(JobName='your-glue-job-name', RunId=job_run_id)
status = status_response['JobRun']['JobRunState']
print("Job status:", status)
