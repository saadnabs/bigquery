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
Date:     2016-11-09
Author:   Nabeel Saad
Desc:     Command-line application, which asynchronously executes jobs via bash/CLI (for BQ and others like Hive).
Use:      TODO: SQL statements are expected in a separate file, semi-colon delimited.
Params:   TODO: GCP / BigQuery project ID, SQL file.
"""

import argparse
import json
import time
import uuid
import subprocess
import sys

from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
from subprocess import Popen, PIPE, CalledProcessError

# [START poll_job]
#TODO: old code
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
#TODO: old code
def build_statements(filename):
    # Open and read the file as a single buffer
    fd = open(filename, 'r')
    sqlFile = fd.read()
    fd.close()

    # all SQL commands (split on ';')
    sqlCommands = (sqlFile[:-1] if sqlFile.endswith(';\n') else sqlFile).split(';\n')

    return sqlCommands
# [END build_statements]

# [START loadCommands]
def loadCommands(filename):
    print("hello load")
    f = open(filename, 'r')
    
    for line in f:
        #Ignore the line that gives an example of the format to be used in the file
        if (line[:1] == "#"):
            continue 
        
        commandComponents = (line[:-1] if line.endswith('\n') else line).split(';')
        if (len(commandComponents) != 3):
            print("ERROR: The format of the file doesn't match the expected format, please follow the categoy;number;command format")
            sys.exit()
        c = Command(commandComponents[0], commandComponents[1], commandComponents[2])
        
        #Store the commands in the list to run
        commands.append(c);
        
# [END loadCommands]

# [START run]
def main(project_id, commandsFile, batch, num_retries, interval):
    loadCommands(commandsFile)
    
    ''''c = Command('simple', 12, '/opt/google-cloud-sdk/bin/bq query --nosync "SELECT COUNT(*) FROM publicdata:samples.wikipedia"')
    print(c.print_command_details())
    c.execute_x_times()
        #TODO: any way to have jobs continue to repeat if they are simple or if long jobs haven't completed?
    
    print_jobs_run()   ''' 
    
    #TODO: poll all jobs and update their times
    
# [END run]
    
     
def print_jobs_run():
    """Prints the dict contains the jobs run, their status and timings"""
    for keys in jobs_run:
        print("jobId--> " + keys)
        for values in jobs_run[keys]:
            print (values,':',jobs_run[keys][values])
        

#Class
class Command:
    """The command object to be used for loading the queries to be run"""
    category = ""
    executable = ""
    timesToExecute = 0

    def __init__(self, category, num, command):
        self.category = category
        self.executable = command
        self.timesToExecute = num
        
    def print_command_details(self):
        print('Command with category[' + self.category + '] timesToExecute[' + str(self.timesToExecute) + '] \n-->executable[' + self.executable + '] ')
    
    def execute_x_times(self):
        for i in range(0, self.timesToExecute):
            try:
                output = subprocess.check_output([self.executable], shell = True)
            except CalledProcessError as exc:
                print("Status: FAIL", exc.returncode, exc.output)
            else:
                #TODO: This is currently BQ specific way of getting job_id
                print(output)
                job_id_location = output.find(default_project_id) + len(default_project_id) + 1
                #print("job_id " + output[job_id_location:])
                jobs_run[output[job_id_location:]] = {'status' : 'started', 'startTime' : '', 'endTime' : '', 'duration' : ''}

#Script defaults that can be set
default_project_id="nsaad-demos"
commands = [] #Used to store the commands loaded from the file
jobs_run = {} #Used to store the IDs of all the jobs run and get their status and details

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
