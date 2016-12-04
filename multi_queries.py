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
import csv
import datetime
import json
import logging
import multiprocessing as mp
import os
import time
import shutil
import subprocess
import sys
#TODO remove, when getting rid of test, get rid of this import
import random
import uuid

from multiprocessing import Process
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
from subprocess import Popen, PIPE, CalledProcessError
from datetime import datetime
from time import sleep

# [START init_bq_api]
def init_bq_api(): 
    
    # Grab the application's default credentials from the environment.
    global credentials, bigquery
    credentials = GoogleCredentials.get_application_default()

    # Construct the service object for interacting with the BigQuery API.
    bigquery = discovery.build('bigquery', 'v2', credentials=credentials)
# [END init_bq_api]

# [START load_commands]
def load_commands(filename):
    f = open(filename, 'r')
    
    com_id = 1
    for line in f:
        #Ignore any commented out lines
        if (line[:1] == "#"):
            continue 
        
        commandComponents = (line[:-1] if line.endswith('\n') else line).split('|')
        if (len(commandComponents) != 3):
            output_log("ERROR: The format of the file doesn't match the expected format, please follow the categoy|number|command format", "true", 40)
            sys.exit()
        
        num_of_executions = int(commandComponents[1]) * int(multiplier)
        
        #Add in duplicate commands up to the number of executions passed in the file 
        for i in range(0, num_of_executions):
            t=""
            command = commandComponents[2]
            
            #if we are running BQAPI command 
            if command.find(tech_options[0]) != -1:
                #Remove 'bqapi "' from the start and the last double quote at the end 
                command = command[7:-1]
                t = tech_options[0]
                
                #If we have a command that uses BQ API, make sure we initiliase it
                global need_bq_api_init
                if need_bq_api_init == False:
                    need_bq_api_init = True
                
            #if we are running BQ CLI command    
            elif command.find(tech_options[1]) != -1:
                
                #Add project ID after BQ command
                bq_location = command.find("bq")
                bq_end = bq_location + len("bq") + 1 #include space in this
                command = command[:bq_end] + '--project_id ' + project_id + " " + command[bq_end:]
                t = tech_options[1]
            
            #if we are running beeline TODO CLI
            elif command.find(tech_options[2]) != -1:
                t = tech_options[2]
                
            #any other tech passed in the commands file gets picked up here
            else:
                t = tech_options[3]
                    
            c = Command(commandComponents[0].strip(), command)
            c.tech = t
                
            #Store the commands in the list to run
            commands.append(c);
            com_id += 1
        
# [END load_commands]

# [START run_jobs]
def run_jobs():

    #Iterate through all the commands
    for command in commands:
        
        cmd = command.executable
        
        #Output command to be run:
        output_log("   |    ", "true", 20)
        output_log("   |--> " + cmd, "true", 20)
        
        #Run each command in a process, handling BQ API differently
        if command.tech == tech_options[0]:
            pool = mp.Pool(10)
        #TODO is still quite slow, figure out multiprocessing 
        #     beeline TBD how to handle it
            pool.apply_async(async_query, args=(bigquery, project_id, cmd))
        else:
            #Split the command appropriately for Popen
            command_args, statement = extract_quoted_sql(cmd) 
            p = subprocess.Popen(command_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            
            #Store the processes to check their output later
            processes.append(p)
        
    output_log("\n", "true", 20)
# [END run_jobs]

def async_query(bigquery, project_id, query):
    
    # Generate a unique job_id
    job_data = {
        'jobReference': {
            'projectId': project_id,
            'job_id': str(uuid.uuid4())
        },
        'configuration': {
            'query': {
                'query': query,
                'priority': 'INTERACTIVE',
                'useQueryCache':'false'
            }
        }
    }
    
    #Get start time
    start = datetime.now()
    
    #TODO confirm removed the return
    out = bigquery.jobs().insert(
        projectId=project_id,
        body=job_data).execute(num_retries=3)
    print("out: " + out)
    end = datetime.now()
    
    #update job result info here with out and append to run jobs
    addNewJobResult(out , start, end)
    sys.exit()
    
# [END async_query]
    
# [START extract_quoted_sql]
def extract_quoted_sql(cmd):
    
    #If we have a quote separating out a SQL statement in BQ
    #TODO BQ specific, or what does a hive call look like? deal with other tech checks here
        #TODO online, check kenneth email with US folks about hive CLI
    if cmd.find('"') != -1:
        #First the double quote to extract the SQL statement
        first_quote = cmd.find('"') + 1
        statement = cmd[first_quote:-1]
    
        #Break up the command options into list
        command_args = cmd[:first_quote - 1].split()
        command_args.append(statement)
    else:
        command_args = cmd.split()
    
    return command_args, statement
# [END extract_quoted_sql]
    
# [START wait_for_processes_and_start_pollers()]
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
                
                #split this to a generic BQ job result update thing / might need something completely different for beeline
                #TODO BQ specific project, ID...
                job_id_newline = out.find("\n")
                out = out[:job_id_newline] #Remove the \ns before printing               
                job_id_location = out.find(project_id) + len(project_id) + 1
                job_id = out[job_id_location:]
                
                #TODO BQ specific, pass in the correct tech in here
                polling_processes.append(subprocess.Popen(["python", "poller.py", str(job_id), "bq"], stdout=subprocess.PIPE, stderr=subprocess.PIPE))
                
                addNewJobResult(job_id, command_start_time, command_end_time)
                
                output_log(str_command_end_time + " " + str(out), "true", 20)
                processes.remove(p)
    
    output_log(" ", "true", 20)
    output_log("Awaiting " + str(len(polling_processes)) + " BigQuery jobs to complete", "true", 20)
# [END wait_for_processes_and_start_pollers()]

def addNewJobResult(job_id, command_start_time, command_end_time):
    #Create a JobResult and start filling in the info
    jb = JobResult(job_id)
    jb.bash_start_time = commands_start_time
    jb.bash_end_time = command_end_time
    jb.bash_duration = int(command_end_time) - int(commands_start_time)
    jobs_run.append(jb)

# [START wait_for_pollers()]
def wait_for_pollers():
    
    while len(polling_processes) > 0:
        for p in polling_processes:
            if p.poll() == 0:
                out, err = p.communicate()
                
                #If the process returns an output
                if out != None: 
                    polling_processes.remove(p)
                    output_log("  |--> waiting for " + str(len(polling_processes)) + " poller(s)", "true", 20)
                    
                    #TODO BQ specific, response would be different
                    #Process the JSON and look for the relevant information.
                    parsedjson = json.loads(out)
                    status = parsedjson['status']
                    state = status['state']
                    
                    if(state == "DONE"):
                        job_reference = parsedjson['jobReference']
                        job_id = job_reference['jobId']
                        
                        #Create a new jobresult with the ID
                        jb = JobResult(job_id);
                        
                        #But check for it in the jobs_running
                        for job in jobs_run:
                            if job.job_id == job_id:
                                jb = job
                                
                        jb.status = state
                        statistics = parsedjson['statistics']
                        jb.error_result = status["errorResult"]
                        jb.bq_creation_time = statistics['creationTime']
                        jb.bq_start_time = statistics['startTime']
                        jb.bq_end_time = statistics['endTime']
                        jb.bq_duration = int(jb.bq_end_time) - int(jb.bq_start_time)
                        jb.bytes_processed = statistics['totalBytesProcessed']
                        
                        configuration = parsedjson['configuration']
                        query = configuration['query']
                        sql_statement = query['query']
                        jb.query_executed = sql_statement
                        
                        jb.category = get_query_category(jb.query_executed)
                        
                        jobs_completed.append(jb)
                        if jb in jobs_run: jobs_run.remove(jb)
# [END wait_for_pollers()]

# [START get_query_category(query)]
def get_query_category(query):
    for c in commands:
        ca, cq = extract_quoted_sql(c.executable)
        if cq == query:
            return c.category
    
    output_log("Somehow non of the loaded commands match the executed query here", None, 30)
# [END get_query_category(query)]    
    
# [START output_completed_jobs]
def output_completed_jobs():
    
    global output_filename
    output_filename = output_file + "-results.csv"
    output_filename = os.path.join(result_path, output_filename)
    
    if not os.path.exists(result_path):
        os.makedirs(result_path)
    
    file_exists = False
    try:
        #If it doesn't throw an error to read, then the file exists; otherwise, create a new file
        f = open(output_filename, 'r')
        #If soo, load it to append
        f = open(output_filename, 'a')
        #And then we know the file already existed
        file_exists = True 
    except IOError as detail:
        if str(detail).find("No such file or directory"):
            try:
                f = open(output_filename, 'wt')
            except IOError:
                output_log("Can not open file to write, check the script's permissions in this directory", "true", 40)
                f.close()
    
    try:
        writer = csv.writer(f, delimiter=";")
        
        if(not file_exists):
            writer.writerow( ('Status', 'BQ Job Duration', 'Bash Job Duration', 'Bytes Processed', 'Bash Job Start Time', 'Bash Job End Time', \
                          'BQ Job Creation Time', 'BQ Job Start Time', 'BQ Job End Time', 'Category', 'Query', 'Job Id', 'Run Id') )
        
        for job in jobs_completed:
            writer.writerow( (job.status, job.bq_duration, job.bash_duration, human_readable_bytes(int(job.bytes_processed)), \
                          date_time_from_milliseconds(job.bash_start_time), date_time_from_milliseconds(job.bash_end_time), \
                          date_time_from_milliseconds(job.bq_creation_time), date_time_from_milliseconds(job.bq_start_time), date_time_from_milliseconds(job.bq_end_time), \
                          job.category, job.query_executed, job.job_id, run_id) )

    finally:
        f.close()
    
    for i in range(0, len(jobs_completed)):
        output_log(jobs_completed[i].print_jobresult_details(), "true", 20)    
# [END output_completed_jobs]

# [START output_log()]
def output_log(message, _print, level): 
    if( _print == "true" and no_console_output != True):
        print(message)
        
    logging.log(level, message)
    
# [END output_log()]

def writeDataLoaderForCharts():
    
    args = ["python","writeDataLoader.py", output_filename]
    p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    count = 0
    while p.poll() != 0:
        sleep(0.5)
        count += 1
        if count > 10:
            print("timed out, breaking...")
            break
    
    out, err = p.communicate()
    if p.poll() == 0: 
        output_log("Chart results updated.", "true", 20)

#Class
class Command:
    """The command object to be used for loading the queries to be run"""
    category = ""
    executable = ""
    tech = ""
        
    def __init__(self, category, command):
        self.category = category
        self.executable = command
        
    def print_command_details(self):
        print('Command with category[' + self.category + '] tech[' + str(self.tech) + '] \n--> executable[' + self.executable + '] ')
#End Class

#Class
class JobResult:
    """The result details for each job run"""
    job_id = 0
    status = ""
    bq_creation_time = ""
    bq_start_time = ""
    bq_end_time = ""
    bq_duration = ""
    bash_start_time = ""
    bash_end_time = ""
    bash_duration = ""
    bytes_processed = ""
    category = ""
    query_executed = ""
    error_result = ""
    
    def __init__(self, job_id):
        self.job_id = job_id
    
    def print_jobresult_job_id(self):
        print('JobResult with job_id[' + self.job_id + ']')
    
    def print_jobresult_details(self):
        return 'JobResult with job_id[' + self.job_id + ']' + "\n" + \
               '  |--> status[' + self.status + ']' + "\n" + \
               '  |--> bq_duration[' + str(self.bq_duration) + '] bq_creation_time[' + str(self.bq_creation_time) + '] bq_start_time[' + date_time_from_milliseconds(self.bq_start_time) + '] bq_end_time[' + date_time_from_milliseconds(self.bq_end_time) + ']' + "\n" + \
               '  |--> bash_duration[' + str(self.bash_duration) + '] bash_start_time[' + date_time_from_milliseconds(self.bash_start_time) + '] bash_end_time[' + date_time_from_milliseconds(self.bash_end_time) + ']' + "\n" + \
               '  |--> bytes_processed[' + human_readable_bytes(int(self.bytes_processed)) + '] category[' + str(self.category) + ']' + "\n" + \
               '  |--> query[' + str(self.query_executed) + ']'
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
path="runs/"
result_path = path + "results/"
tech_options = ["bqapi", "bq", "beeline", "other"]
need_bq_api_init = False
  
# [START run]
def main(commandsFile):
    load_commands(commandsFile)
    
    #Output configuration information
    output_log("++++++++++++++++++++++++++++++++++++++++++++++++++++++++", "true", 20)
    output_log("Run with configuration: ", "true", 20)
    output_log("  |--> command_file: " + commandsFile, "true", 20)
    output_log("  |--> project_id: " + project_id, "true", 20)
    output_log("  |--> multiplier: " + str(multiplier), "true", 20)
    output_log("  |--> run id: " + str(run_id), "true", 20)
    output_log("--------------------------------------------------------\n\n", "true", 20)
            
    #Get the start time of all commands
    global commands_start_time
    commands_start_time = int(round(time.time() * 1000))
    output_log(str(datetime.now()) + " -- Starting parallel scripts: ", "true", 20)
    
    if need_bq_api_init == True:
        init_bq_api()
    
    run_jobs()
    wait_for_processes_and_start_pollers()
    wait_for_pollers()
    
    output_log("\n--------------------------------------------------------", "true", 20)
    output_log(str(datetime.now()) + " -- Job Results", "true", 20)
    output_log("--------------------------------------------------------\n", "true", 20)
    output_completed_jobs();
    output_log("\n--------------------------------------------------------\n", "true", 20)
    output_log("End of run with id: " + run_id, "true", 20)
    output_log("++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n\n", "true", 20)
    
    writeDataLoaderForCharts()
    shutil.copyfile(commandsFile, result_path + output_file + "-commands.file")

# [END run]   
  
# [START main]
if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('commandsFile', help='Delimited file containing the commands to run.')
    parser.add_argument('project_id', help='Project ID to use.', default="nsaad-demos")
    parser.add_argument('output_file', nargs="?", help='Name of the file to use to output the log/results.', default=datetime.now().strftime("%Y-%m-%d-%H-%M"))
    parser.add_argument('multiplier', nargs="?", help='A multiplier to be used to increase the executes of the commands by that multiplier.')
    parser.add_argument('-nco', '--no_console_output', action='store_true', help='A multiplier to be used to increase the executes of the commands by that multiplier.')
    
    args = parser.parse_args()
    
    #Set the global variables from the params
    global project_id, output_file, multiplier, no_console_output, run_id
    project_id = args.project_id
    output_file = args.output_file
    
    #Set up the logging
    log_path = path + "logs/" 
    output_filename = os.path.join(log_path, output_file)
    
    if not os.path.exists(log_path):
        os.makedirs(log_path)
    
    logging.basicConfig(filename=output_filename + '-output.log', level=logging. DEBUG)
    
    #Set the correct "no console output" value
    no_console_output = args.no_console_output

    #Set a default value for multiplier to 1 if not passed in
    if (args.multiplier):
        multiplier = args.multiplier
    else: 
        multiplier = 1
    
    #ID just to distinguish between different runs of this script if run via the query_load_over_time script
    run_id = str(args.commandsFile) + "-" + str(project_id) + "-" + str(multiplier)
    
    main(
        args.commandsFile)
# [END main]