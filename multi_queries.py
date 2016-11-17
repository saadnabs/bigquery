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
#TODO if remove test, get rid of this import
import random

from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
from subprocess import Popen, PIPE, CalledProcessError
from datetime import datetime
from time import sleep

# [START load_commands]
def load_commands(filename):
    f = open(filename, 'r')
    
    com_id = 1
    for line in f:
        #Ignore the line that gives an example of the format to be used in the file
        if (line[:1] == "#"):
            continue 
        
        commandComponents = (line[:-1] if line.endswith('\n') else line).split(';')
        if (len(commandComponents) != 3):
            output_log("ERROR: The format of the file doesn't match the expected format, please follow the categoy;number;command format", true, true, 3)
            sys.exit()
        
        num_of_executions = int(commandComponents[1]) * multiplier
        
        #Add in duplicate commands up to the number of executions passed in the file 
        for i in range(0, num_of_executions):
            
            #if we are running BQ command 
            if commandComponents[2].find("bq") != -1:
                command = commandComponents[2]
                bq_location = commandComponents[2].find("bq")
                bq_end = bq_location + len("bq") + 1 #include space in this
                command = command[:bq_end] + '--project_id ' + project_id + " " + command[bq_end:]
            
            if commandComponents[0].find("test") != -1:
                command = commandComponents[2] + " " + str(random.randrange(0, 10, 1)) + ";echo 'done'"
                
            c = Command(commandComponents[1], command)
                
            #Store the commands in the list to run
            commands.append(c);
            com_id += 1
            #TODO do something with the type of query
        
# [END load_commands]

# [START forkProcesses]
def run_jobs():

    #TODO break the starting of queries over a period of time
    #Iterate through all the commands
    for command in commands:
        
        #Split the command appropriately for Popen
        cmd = command.executable
        #If we have a quote separating out a SQL statement in BQ
        #TODO: BQ specific, deal with other tech checks here
        if cmd.find('"') != -1:
            #First the double quote to extract the SQL statement
            first_quote = cmd.find('"') + 1
            statement = cmd[first_quote:-1]
        
            #Break up the command options into list
            command_args = cmd[:first_quote - 1].split()
            command_args.append(statement)
        else:
            command_args = cmd.split()
        
        output_log("   |    ", true, true, 1)
        output_log("   |--> " + cmd, true, true, 1)
        
        #Run each command in a process
        p = subprocess.Popen(command_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        #Store the processes to check their output later
        processes.append(p)
        
    output_log("\n", true, true, 1)
    
def wait_for_processes_and_start_pollers():
    
    #FR: add in ability to only have X number of pollers spun up so that they don't exhaust a machine if too many jobs are running, have a pool and spin up more as needed
    #    or maybe this FR is about using multiprocessing to call the inside code via separate thread -- making sure those threads have access to: project_id, jobs_run
    #    it would have to use a thread pool otherwise I'd have tons of threads spinning up like crazy  
    #Check the status of the bash shell processes, and get their output
    while len(processes) > 0:
        for p in processes:
            #When the process has completed and returned a success exit code
            if p.poll() == 0:
                out, err = p.communicate()
                
                command_end_time = int(round(time.time() * 1000)) 
                str_command_end_time = str(datetime.now())
                #TODO: project ID is BQ specific
                job_id_newline = out.find("\n")
                out = out[:job_id_newline] #Remove the \ns before printing               
                job_id_location = out.find(project_id) + len(project_id) + 1
                job_id = out[job_id_location:]
                
                #TODO: BQ specific, pass in the correct tech in here
                polling_processes.append(subprocess.Popen(["python", "poller.py", str(job_id), "bq"], stdout=subprocess.PIPE, stderr=subprocess.PIPE))
                
                #Create a JobResult and start filling in the info
                jb = JobResult(job_id)
                jb.bash_start_time = commands_start_time
                jb.bash_end_time = command_end_time
                jb.bash_duration = int(command_end_time) - int(commands_start_time)
                jobs_run.append(jb)
                
                output_log(str_command_end_time + " " + str(out), true, true, 1)
                processes.remove(p)
    
    output_log("\nAwaiting " + str(len(polling_processes)) + " BigQuery jobs to complete", true, true, 1)
    #TODO bring outputs back in here, and output stuff as we wait
# [END forkProcesses]

def wait_for_pollers():
    
    while len(polling_processes) > 0:
        for p in polling_processes:
            if p.poll() == 0:
                out, err = p.communicate()
                
                #If the process returns an output
                if out != None: 
                    polling_processes.remove(p)
                    output_log("  |--> waiting for " + str(len(polling_processes)) + " poller(s)", true, true, 1)
                    
                    #TODO: This is BQ specific
                    #Process the JSON and look for the relevant information.
                    parsedjson = json.loads(out)
                    status = parsedjson['status']
                    state = status['state']
                    
                    if(state == "DONE"):
                        job_reference = parsedjson['jobReference']
                        job_id = job_reference['jobId']
                        
                        jb = JobResult(job_id)
                        jb.status = state
                        statistics = parsedjson['statistics']
                        jb.bq_start_time = statistics['startTime']
                        jb.bq_end_time = statistics['endTime']
                        jb.bq_duration = int(jb.bq_end_time) - int(jb.bq_start_time)
                        jb.bytes_processed = statistics['totalBytesProcessed']
                        
                        jobs_completed.append(jb)
                        if jb in jobs_run: jobs_run.remove(jb)

# [START output_completed_jobs]
def output_completed_jobs():
    
    output_filename = output_file + "-results.csv"
    
    #TODO: check if there's a more elegant way to do an (append or write) file
    try:
        f = open(output_filename, 'r')
        f = open(output_filename, 'a')
        text = ('another', 'line', 'here')
    except IOError as detail:
        if str(detail).find("No such file or directory"):
            try:
                f = open(output_filename, 'wt')
                text = ('my', 'text', 'here')
            except IOError:
                output_log("Can not open file to write, check the script's permissions in this directory", true, true, 3)
                f.close()
    
    try:
        writer = csv.writer(f)
        writer.writerow( ('Status', 'BQ Job Duration', 'Bash Job Duration', 'Bytes Processed', 'BQ Job Start Time', 'BQ Job End Time' , \
                          'Bash Job Start Time', 'Bash Job End Time', 'Job Id') )
        for job in jobs_completed:
            writer.writerow( (job.status, job.bq_duration, job.bash_duration, human_readable_bytes(int(job.bytes_processed)), \
                              date_time_from_milliseconds(job.bq_start_time), date_time_from_milliseconds(job.bq_end_time), \
                              date_time_from_milliseconds(job.bash_start_time), date_time_from_milliseconds(job.bash_end_time), \
                              job.job_id) )
    finally:
        f.close()
    
    for i in range(0, len(jobs_completed)):
        jobs_completed[i].print_jobresult_details()    
# [END output_completed_jobs]

# [START output_log()]
def output_log(message, _print, log, level):
    if _print != NONE:
        print(message)
        
    #TODO create enumarator _LOG
    if log != NONE and level == _LOG.level: 
        append_to_log(text)
    
# [END output_log()]

# [START append_to_log(text)]
def append_to_log(text):
    output_filename = output_file + "-output.log"
    
    #TODO: check if there's a more elegant way to do an (append or write) file
    try:
        f = open(output_filename, 'r')
        f = open(output_filename, 'a')
    except IOError as detail:
        if str(detail).find("No such file or directory"):
            try:
                f = open(output_filename, 'wt')
            except IOError:
                print("Can not open file to write, check the script's permissions in this directory")
                f.close()
    
    try:
        #TODO is this right, sys.writer just for normal text?, what method to right? or can f write directly?
        writer = sys.writer(f)
        writer.write( text )
        
    finally:
        f.close()
# [END append_to_log(text)]

#Class
class Command:
    """The command object to be used for loading the queries to be run"""
    category = ""
    executable = ""
        
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
    bq_start_time = ""
    bq_end_time = ""
    bq_duration = ""
    bash_start_time = ""
    bash_end_time = ""
    bash_duration = ""
    bytes_processed = ""
    
    def __init__(self, job_id):
        self.job_id = job_id
    
    def print_jobresult_job_id(self):
        print('JobResult with job_id[' + self.job_id + ']')
    
    def print_jobresult_details(self):
        print('JobResult with job_id[' + self.job_id + ']')
        print( '  |--> status[' + self.status + ']' )
        print( '  |--> bq_duration[' + str(self.bq_duration) + '] bq_start_time[' + date_time_from_milliseconds(self.bq_start_time) + '] bq_end_time[' + date_time_from_milliseconds(self.bq_end_time) + ']')
        print( '  |--> bash_duration[' + str(self.bash_duration) + '] bash_start_time[' + date_time_from_milliseconds(self.bash_start_time) + '] bash_end_time[' + date_time_from_milliseconds(self.bash_end_time) + ']')
        print ( ' |--> bytes_processed[' + human_readable_bytes(int(self.bytes_processed)) + ']')  
#End Class

def date_time_from_milliseconds(ms):
    s, ms = divmod(int(ms), 1000)
    return '%s.%03d' % (time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(s)), ms)

def human_readable_bytes(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix) 

#Script defaults that can be set
commands_start_time = ""
commands = [] #Used to store the commands loaded from the file
jobs_run = [] #Used to store the job results of running jobs
jobs_completed = [] #Used to store the job results of jobs that have been confirmed completed.
processes = [] #Used to store the processes launched in parallel to run all the commands
polling_processes = [] #Used to store the processes running the pollers for each job
#TODO python enumarator?
#Is there a python logging module?
_LOG.level = {INFO:1, WARNING:2, ERROR:3}
  
# [START run]
def main(commandsFile):
    load_commands(commandsFile)

    print("*******************************************************")
    print(str(datetime.now()) + " -- Starting parallel bash scripts: ")
            
    #Get the start time of all commands
    global commands_start_time
    commands_start_time = int(round(time.time() * 1000))
    
    run_jobs()
    wait_for_processes_and_start_pollers()
    wait_for_pollers()
    
    print("\n*******************************************************")
    print(str(datetime.now()) + " -- Job Results")
    print("*******************************************************\n")
    output_completed_jobs();
    print("\n*******************************************************\n")

# [END run]   
  
# [START main]
if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('commandsFile', help='Delimited file containing the commands to run.')
    parser.add_argument('project_id', help='Project ID to use.', default="nsaad-demos")
    parser.add_argument('output_file', nargs="?", help='Name of the file to use to output the log/results.', default=str(datetime.now()))
    parser.add_argument('multiplier', nargs="?", help='A multiplier to be used to increase the executes of the commands by that multipler.')
    #TODO add argument flag for "no_console_output", when passed in, set output_to_console = None
    
    args = parser.parse_args()
    global project_id, output_file, multiplier
    project_id = args.project_id
    output_file = args.output_file
    
    #multiplier = args.multiplier ? args.multiplier : 1
    #TODO find shorter version of this code
    if (args.multiplier):
        multipler = args.multiplier
    else: 
        multiplier = 1
    
    main(
        args.commandsFile)
# [END main]