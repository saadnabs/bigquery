#!/usr/bin/env python

# Copyright 2015, Google, Inc.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Date:     08/06/16
Author:   Rich Radley
Desc:     Command-line application, which asynchronously executes SQL statements  in BigQuery.
Use:      SQL statements are expected in a separate file, semi-colon delimited.
Params:   GCP / BigQuery project ID, SQL file.
"""

import argparse
import json
import time
import uuid
import subprocess

from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
from subprocess import Popen, PIPE, CalledProcessError


# [START async_query]
def async_query(bigquery, project_id, query, batch=False, num_retries=5):
    # Generate a unique job_id
    job_data = {
        'jobReference': {
            'projectId': project_id,
            'job_id': str(uuid.uuid4())
        },
        'configuration': {
            'query': {
                'query': query,
                'priority': 'BATCH' if batch else 'INTERACTIVE',
                'useQueryCache':'false'
            }
        }
    }
    return bigquery.jobs().insert(
        projectId=project_id,
        body=job_data).execute(num_retries=num_retries)
# [END async_query]


# [START poll_job]
def poll_job(bigquery, job):
    """Waits for a job to complete."""

    print('Waiting for job to finish...')

    request = bigquery.jobs().get(
        projectId=job['jobReference']['projectId'],
        jobId=job['jobReference']['jobId'])

    while True:
        result = request.execute(num_retries=2)

        if result['status']['state'] == 'DONE':
            if 'errorResult' in result['status']:
                raise RuntimeError(result['status']['errorResult'])
            print('Job complete.')
            return

        time.sleep(1)
# [END poll_job]

# [START build_statements]
def build_statements(filename):
    # Open and read the file as a single buffer
    fd = open(filename, 'r')
    sqlFile = fd.read()
    fd.close()

    # all SQL commands (split on ';')
    sqlCommands = (sqlFile[:-1] if sqlFile.endswith(';\n') else sqlFile).split(';\n')

    return sqlCommands
# [END build_statements]

# [START run]
def main(project_id, commandsFile, batch, num_retries, interval):
    try:
        output = subprocess.check_output(['/opt/google-cloud-sdk/bin/bq query --nosync "SELECT COUNT(*) FROM publicdata:samples.wikipedia"'], shell = True)
    except CalledProcessError as exc:
        print("Status: FAIL", exc.returncode, exc.output)
    else:
        print("ls output: " + output)
        
    print('project_id ' + project_id)
    
'''
def main(project_id, sqlFile, batch, num_retries, interval):
    # [START build_service]
    # Grab the application's default credentials from the environment.
    credentials = GoogleCredentials.get_application_default()

    # Construct the service object for interacting with the BigQuery API.
    bigquery = discovery.build('bigquery', 'v2', credentials=credentials)
    # [END build_service]

    sql = build_statements(sqlFile)

    for statement in sql:
        try:
        # Submit the job and wait for it to complete.
            query_job = async_query(
                bigquery,
                project_id,
                statement,
                batch,
                num_retries)

            print('Job started: ' + query_job['jobReference']['jobId'])

        # poll_job(bigquery, query_job)

        except Exception, e:
            print "Error: ", e
'''
# [END run]

#Script defaults that can be set
default_project_id="nsaad-demos"

# [START main]
if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('project_id', nargs='?', help='Your Google Cloud project ID.', default=default_project_id)
    parser.add_argument('commandsFile', help='Delimited file containing the commands to run.')
    parser.add_argument(
        '-b', '--batch', help='Run query in batch mode.', action='store_true')
    parser.add_argument(
        '-r', '--num_retries',
        help='Number of times to retry in case of 500 error.',
        type=int,
        default=5)
    parser.add_argument(
        '-p', '--poll_interval',
        help='How often to poll the query for completion (seconds).',
        type=int,
        default=1)

    args = parser.parse_args()

    main(
        args.project_id,
        args.commandsFile,
        args.batch,
        args.num_retries,
        args.poll_interval)
# [END main]
