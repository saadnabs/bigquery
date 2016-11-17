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
Use:      Commands to run are expected in a separate file, each command on a new line, semi-colon delimited with format:
          category; number of times to run; command
Params:   GCP / BigQuery project ID, file containing commands to run
"""

import argparse
import json
import time
import uuid
import subprocess
import sys
import datetime
import json
import csv
import random
#TODO: get rid of psutil when test version is removed
import psutil


from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
from subprocess import Popen, PIPE, CalledProcessError
from datetime import datetime
from time import sleep
     
# [START poll_running_job] 
#TODO get rid of test version
def poll_running_test_job(job_id, project_id):
    #TODO BQ specific polling
    """Polls the job running, and returns the status and statistics """
    
    status = ""
    p = psutil.Process(int(job_id))
    while p.poll() is None:
        sleep(0.5)
    
    print(str(random_length) + "{status:{'state': 'done'}, statistics:{'startTime':'11223344', 'endTime':'11443322', 'totalBytesProcessed':'4096000'}}")
# [END poll_running_job]

def poll_running_bq_job(job_id, project_id):
    #TODO BQ specific polling
    """Polls the job running, and returns the status and statistics """
    
    status = ""
    while status == "":
        processes = []
        processes_outputs = []
        
        command_args = ['bq', '--project_id', project_id, '--format', 'json', 'wait', job_id, '1']
        processes.append(subprocess.Popen(command_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE))
        
        for p in processes:
            out, err = p.communicate()
            if out != "" and out.find("Wait timed out") == -1:
                try:
                    parsedjson = json.loads(out)
                    status = parsedjson['status']
                    state = status['state']
                    if (state == "DONE"):
                        status == "DONE"
                        print(out)
                except ValueError as detail:
                    print("No JSON was returned: " + detail)
            sleep(1)

#Script defaults that can be set
processes = [] #Used to store the processes launched in parallel to run all the commands
processes_outputs = []
  
# [START run]
def main(job_id, project_id, tech):
    #Store the project_id global to be used in the script
    #print("**Polling job: [" + job_id + "]")
        
    #TODO get rid of test version?
    if tech == "test":
        #print("using test configuration")
        poll_running_test_job(job_id, project_id)
    elif tech == "bq":
        #print("using bq configuration")
        poll_running_bq_job(job_id, project_id)
    else:
        print("other tech, to deal with or genericise polling")
# [END run]   
  
# [START main]
if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('job_id', help='The ID of the job to poll.')
    parser.add_argument('tech', nargs='?', help='The technology being tested, bq or hive', default="test")
    parser.add_argument('project_id', nargs='?', help='GCP project ID to use to poll for the running query.', default="nsaad-demos")
    
    args = parser.parse_args()

    main(
        args.job_id, args.project_id, args.tech)
# [END main]