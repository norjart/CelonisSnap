from os.path import isfile, join
from os import listdir
import requests
from pathlib import Path
print('Running' if __name__ == '__main__' else 'Importing', Path(__file__).resolve())


class cloud:
    # Returns the API URL
    def get_api(self, path):
        return "https://{}.{}.celonis.cloud/{}".format(self.tenant, self.realm, path)

    def __init__(self, tenant, realm, api_key):
        self.tenant = tenant
        self.realm = realm
        self.api_key = api_key

    # Returns list jobs endpoint
    def get_jobs_api(self, pool_id):
        return self.get_api("integration/api/v1/data-push/{}/jobs/".format(pool_id))

    # Returns authorization json which must be in every API call
    def get_auth(self):
        return {'authorization': "Bearer {}".format(self.api_key)}

    # Calls list jobs endpoint
    def list_jobs(self, pool_id):
        api = self.get_jobs_api(pool_id)
        return requests.get(api, headers=self.get_auth()).json()

    # Deletes a job by pool_id and job_id
    def delete_job(self, pool_id, job_id):
        api = self.get_jobs_api(pool_id) + "/{}".format(job_id)
        return requests.delete(api, headers=self.get_auth())

    # Creates a job by pool_id
    # If you add a data_connection_id than the job is generated for this specific data connection, else it gets created globally
    # Creates a job by pool_id
    # If you add a data_connection_id than the job is generated for this specific data connection, else it gets created globally

    def create_job(self, pool_id, targetName, data_connection_id="", upsert=False):
        api = self.get_jobs_api(pool_id)
        # Set Job-Type. Can be REPLACE (which deletes existing data) or UPSERT (which merges the old data with the new).
        job_type = "REPLACE"
        if upsert:
            job_type = "DELTA"
        if not data_connection_id:
            payload = {'targetName': targetName, 'type': job_type,
                       'dataPoolId': pool_id, 'fileType': 'CSV'}
        else:
            payload = {'targetName': targetName, 'type': job_type,
                       'dataPoolId': pool_id, 'dataConnectionId': data_connection_id, 'fileType': 'CSV'}
        return requests.post(api, headers=self.get_auth(), json=payload).json()

    # Push csv file to job_id and pool
    def push_new_chunk(self, pool_id, job_id, file_path):
        api = self.get_jobs_api(pool_id) + "/{}/chunks/upserted".format(job_id)
        upload_file = {"file": open(file_path, "rb")}
        return requests.post(api, files=upload_file, headers=self.get_auth())

    # Start job
    def submit_job(self, pool_id, job_id):
        api = self.get_jobs_api(pool_id) + "/{}/".format(job_id)
        return requests.post(api, headers=self.get_auth())
