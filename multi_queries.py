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
            
            #if we are running BQ command 
            if commandComponents[2].find("bq") != -1:
                command = commandComponents[2]
                bq_location = commandComponents[2].find("bq")
                bq_end = bq_location + len("bq") + 1 #include space in this
                command = command[:bq_end] + '--project_id ' + project_id + " " + command[bq_end:]
                
            #c = Command(commandComponents[0], commandComponents[2])
            #Store the commands in the list to run
            commands.append(command);
            #TODO do something with the type of query
        
# [END loadCommands]

# [START forkProcesses]
def forkProcesses():

    #TODO break the starting of queries over a period of time
    #Attempting to get wait to work by not using shell=True
    for cmd in commands:
        #First the double quote to extract the SQL statement
        first_quote = cmd.find('"') + 1
        statement = cmd[first_quote:-1]
        
        #Break up the command options into list
        command_args = cmd[:first_quote - 1].split()
        command_args.append(statement)
        
        #Run each command in a process and store the process details
        processes.append(subprocess.Popen(command_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE))
        
    for p in processes:
        p.wait() #Wait for the processes to finish
        out, err = p.communicate()
        processes_outputs.append(out)
        
    #TODO bring outputs back in here, and output stuff as we wait
# [END forkProcesses]

# [START forkOutputs]
def forkOutputs():
    for out in processes_outputs:
        job_id_newline = out.find("\n")
        out = out[:job_id_newline] #Remove the \ns before printing
        print(str(datetime.now()) + " " + str(out))
        
        job_id_location = out.find(project_id) + len(project_id) + 1
        job_id = out[job_id_location:]
        jb = JobResult(job_id)
        jobs_run.append(jb)
# [END forkOutputs]
     
# [START poll_jobs_run] 
def poll_jobs_run():
    #TODO BQ specific polling
    """Polls the list of jobs run, and updates their status and statistics when they are complete and moves them to jobs_completed"""
    while len(jobs_run) > 0:
        processes = []
        processes_outputs = []
        for jb in jobs_run:
            command_args = ['bq', '--project_id', project_id, '--format', 'json', 'wait', jb.job_id, '1']
            processes.append(subprocess.Popen(command_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE))
        
        for p in processes:
            p.wait() #Wait for the processes to finish
            out, err = p.communicate()
            if out != "":
                processes_outputs.append(out)
            else:
                processes_outputs.append(err)
           
        i = 0 
        #TODO BQ specific
        for jb in jobs_run[:]:
            output = processes_outputs[i]
            if(output.find("Wait timed out") == -1):
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
                    if jb in jobs_run: jobs_run.remove(jb)
            i += 1
        
        print("\njobs_running:" + str(len(jobs_run)) + "  jobs_completed: " + str(len(jobs_completed)))
        
        sleep(1.5)
        #FR-01: If timeout passed in, quit the loop after X times polled.
# [END poll_jobs_run]

# [START output_completed_jobs]
def output_completed_jobs():
    
    output_filename = str(datetime.now()) + "-results.csv"
    f = open(output_filename, 'wt')
    try:
        writer = csv.writer(f)
        writer.writerow( ('Status', 'Duration', 'Bytes Processed', 'Start Time', 'End Time' , 'Job Id') )
        for job in jobs_completed:
            writer.writerow( (job.status, job.duration, human_readable_bytes(int(job.bytes_processed)), \
                              date_time_from_milliseconds(job.start_time), \
                              date_time_from_milliseconds(job.end_time), job.job_id) )
    finally:
        f.close()
    
    for i in range(0, len(jobs_completed)):
        jobs_completed[i].print_jobresult_details()    
# [END output_completed_jobs]

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
        print('JobResult with job_id[' + self.job_id + '] status[' + self.status + '] start_time[' + date_time_from_milliseconds(self.start_time) + 
              '] end_time[' + date_time_from_milliseconds(self.end_time) + '] duration[' + str(self.duration) + '] bytes_processed[' + human_readable_bytes(int(self.bytes_processed)) + ']')  
#End Class

def human_readable_bytes(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)  

def date_time_from_milliseconds(ms):
    s, ms = divmod(int(ms), 1000)
    return '%s.%03d' % (time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(s)), ms)

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
  
# [START run]
def main(proj_id, commandsFile, batch, num_retries, interval):
    #Store the project_id global to be used in the script
    global project_id
    project_id = proj_id

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
# [END run]   
  
# [START main]
if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('project_id', nargs='?', help='Your Google Cloud project ID.', default=default_project_id)
    parser.add_argument('commandsFile', help='Delimited file containing the commands to run.')
    #TODO remove unnecessary arguments / add option for hive vs bq
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