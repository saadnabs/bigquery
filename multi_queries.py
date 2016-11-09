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

from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
from subprocess import Popen, PIPE, CalledProcessError
from datetime import datetime
from time import sleep

# [START loadCommands]
def loadCommands(filename):
    f = open(filename, 'r')
    
    for line in f:
        #Ignore the line that gives an example of the format to be used in the file
        if (line[:1] == "#"):
            continue 
        
        commandComponents = (line[:-1] if line.endswith('\n') else line).split(';')
        if (len(commandComponents) != 3):
            print("ERROR: The format of the file doesn't match the expected format, please follow the categoy;number;command format")
            sys.exit()
        #c = Command(commandComponents[0], commandComponents[1], commandComponents[2])
        for i in range(0, int(commandComponents[1])):
            #c = Command(commandComponents[0], commandComponents[2])
        
            #Store the commands in the list to run
            commands.append(commandComponents[2]);
        
# [END loadCommands]

# [START forkProcesses]
def forkProcesses():

    #Attempting to get wait to work by not using shell=True
    for cmd in commands:
        #First the double quote to extract the SQL statement
        first_quote = commands[0].find('"') + 1
        statement = commands[0][first_quote:-2]
        
        #Break up the command options into list
        command_args = commands[0][:first_quote - 1].split()
        command_args.append(statement)
        
        #Run each command in a process and store the process details
        processes.append(subprocess.Popen(command_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE))
        
    for p in processes:
        p.wait() #Wait for the processes to finish
        out, err = p.communicate()
        
        processes_outputs.append(out)
# [END forkProcesses]

# [START forkOutputs]
def forkOutputs():
    for out in processes_outputs:
        job_id_newline = out.find("\n")
        out = out[:job_id_newline] #Remove the \ns before printing
        print(str(datetime.now()) + " " + str(out))
        
        job_id_location = out.find(default_project_id) + len(default_project_id) + 1
        job_id = out[job_id_location:]
        jb = JobResult(job_id)
        jobs_run.append(jb)
# [END forkOutputs]

# [START run]
def main(project_id, commandsFile, batch, num_retries, interval):
    loadCommands(commandsFile)
        
    print("*******************************************************")
    print(str(datetime.now()) + " -- Time command running started: ")
    commands_start_time = int(round(time.time() * 1000))
    
    forkProcesses()
    
    commands_end_time = int(round(time.time() * 1000))
    
    forkOutputs()
    
    print(str(datetime.now()) + " -- Time command running ended: ")
    print("--> Commands ran in " + str((commands_end_time - commands_start_time)))
    print("*******************************************************\n")
        
    poll_jobs_run()
    
    print("\n*******************************************************")
    print(str(datetime.now()) + " -- Job Results")
    print("*******************************************************\n")
    output_completed_jobs();
    ''''
    #Creating a single command
    c = Command('simple', 12, '/opt/google-cloud-sdk/bin/bq query --nosync "SELECT COUNT(*) FROM publicdata:samples.wikipedia"')
    print(c.print_command_details())
    c.execute_x_times()
        #TODO: any way to have jobs continue to repeat if they are simple or if long jobs haven't completed?
    
    print_jobs_run()   '''
    
# [END run]

def output_completed_jobs():
    for i in range(0, len(jobs_completed)):
        jobs_completed[i].print_jobresult_details()

# [START get_jobresult_with_id]
def get_jobresult_with_id(job_id):
    for i in range(0, len(jobs_run)):
        if jobs_run[i].job_id == job_id:
            return jobs_run[i]
    return NONE

# [END get_jobresult_with_id]   
     
def poll_jobs_run():
    """Polls the list of jobs run, and updates their status and statistics when they are complete and moves them to jobs_completed"""
    while len(jobs_run) > 0: 
        for item in jobs_run[:]:     
            jb = item      
            #TODO: This is currently BQ specific way of getting job_id
            try:
                output = subprocess.check_output(['bq', '--format', 'json', 'wait', jb.job_id, '1'])
            except CalledProcessError as exc:
                if(exc.output.find("Wait timed out") == -1):
                    print("Status: FAIL", exc.returncode, exc.output)
                pass #Ignore exceptions about time outs as we are waiting till bq wait says the job is done
            else:
                parsedjson = json.loads(output)
                status = parsedjson['status']
                state = status['state']
                
                if(state == "DONE"):
                    jb.status = "DONE"
                    
                    statistics = parsedjson['statistics']
                    jb.start_time = statistics['startTime']
                    jb.end_time = statistics['endTime']
                    jb.bytes_processed = statistics['totalBytesProcessed']
                    jb.duration = int(jb.end_time) - int(jb.start_time)
                    
                    jobs_completed.append(jb)
                    jobs_run.remove(jb)
    
        print("\njobs_run:" + str(len(jobs_run)) + "  jobs_completed: " + str(len(jobs_completed)))
        
        sleep(1)
        #FR-01: If timeout passed in, quit the loop after X times polled.
    
#Class
class Command:
    """The command object to be used for loading the queries to be run"""
    category = ""
    executable = ""
    #timesToExecute = 0

    #def __init__(self, category, num, command):
    def __init__(self, category, command):
        self.category = category
        self.executable = command
        
    def print_command_details(self):
        print('Command with category[' + self.category + '] timesToExecute[' + str(self.timesToExecute) + '] \n--> executable[' + self.executable + '] ')
#End Class

def human_readable_bytes(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)  

#Class
class JobResult:
    """The result details for each job run"""
    job_id = 0
    status = ""
    start_time = ""
    duration = ""
    end_time = ""
    bytes_processed = ""
    
    def __init__(self, job_id):
        self.job_id = job_id
    
    def print_jobresult_job_id(self):
        print('JobResult with job_id[' + self.job_id + ']')
    
    def print_jobresult_details(self):
        print('JobResult with job_id[' + self.job_id + '] status[' + self.status + '] start_time[' + str(self.start_time) + 
              '] end_time[' + str(self.end_time) + '] duration[' + str(self.duration) + '] bytes_processed[' + human_readable_bytes(int(self.bytes_processed)) + ']')  
#End Class

#Script defaults that can be set
default_project_id="nsaad-demos"
commands_start_time = 0
commands_end_time = 0
commands = [] #Used to store the commands loaded from the file
processes = [] #Used to store the processes launched in parallel to run all the commands
processes_outputs = []
jobs_run = [] #Used to store the job results of running jobs
jobs_completed = [] #Used to store the job results of jobs that have been confirmed completed.
#jobs_run = {} #Used to store the IDs of all the jobs run and get their status and details
  
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

# [END main]
