import argparse
import datetime
import io
import math
import subprocess
import sys  

from datetime import datetime
from io import StringIO
from time import sleep
from subprocess import Popen, PIPE, CalledProcessError

# [START calculate_time_in_seconds]
def calculate_time_in_seconds(time_period):
    #Extract the last character of the time-period to assess the unit
    unit_char = str.lower(time_period[-1:])
    #print("unit char: " + unit_char)
    try:
        int(unit_char)
    except:
        time_period = time_period[:-1]
        #print("time_period without unit " + time_period)
    
    #This will be the time multiplier setting the time in seconds accurately
    time_multiplier = 1
    
    #Handle the time unit appropriately
    if unit_char == "m":
        time_multiplier = 60
        print("Using time period in minutes, running for " + time_period + " minute(s)")
    elif unit_char == "h":
        print("Using time period in hours, running for " + time_period + " hour(s)")
        time_multiplier = 3600
    elif unit_char == "d":
        print("Using time period in days, running for " + time_period + " day(s)")
        time_multiplier = 86400
    #else:
        #if unit_char != "s": print("Assuming time period in seconds, running for " + time_period + " second(s)")
        
    #Set the time_period to the appropriate number of seconds based on the unit
    time_period = int(time_period) * time_multiplier
    #print("time_period: " + str(time_period))
# [END calculate_time_in_seconds]

# [START increase_multiplier]
def increase_multiplier(current_multiplier):

    #Set to lower case to ignore case
    global exp_multiplier, multiplier
    multiplier = str.lower(multiplier)
    
    if multiplier == "increment":
        current_multiplier = current_multiplier + 1 #1, 2, 3, 4, 5
    elif multiplier == "step2":
        current_multiplier =  exp_multiplier * 2 #1, 2, 4, 6, 8, 10, ...
        exp_multiplier += 1
    elif multiplier == "exponential":
        
        #Making exp_multiplier a global variable to retain the increment
        current_multiplier =  math.pow(2, exp_multiplier) #1, 2, 4, 8, 16, 32
        exp_multiplier += 1
    else:
        print("No multiplier provided or not a valid option, defaulting to incremental")
        current_multiplier =  current_multiplier + 1
        
    if current_multiplier >= multiplier_cap:
        return multiplier_cap
    else:
        return current_multiplier
    
# [END increase_multiplier]

# [START run]
def main(commands_file):
    
    calculate_time_in_seconds(time_period)

    print("*******************************************************")
    print(str(datetime.now()) + " -- Starting query load generation: ")
    print("*******************************************************")
            
    time_counter = 0
    m = 1 #Multiplier
    while int(time_counter) < int(time_period):
        #print("time_counter " + str(time_counter) + "  time_period " + str(time_period))
        
        if (time_counter % ramp_up_period) == 0:
            #print("hit ramp up cycle number [" + str((time_counter / ramp_up_period) + 1) + "]")
            print("At " + str(time_counter) + " second(s) hit ramp up cycle " + str((time_counter / ramp_up_period) + 1) + " with multiplier at " + str(int(m)))

            #Append no_console_output if it's passed into this script         
            args = ["python","multi_queries.py", commands_file, project_id, output_file, str(m)]
            if no_console_output is not None and no_console_output != "": args.append("-nco")
            if legacy_sql is not None and legacy_sql != False: args.append("-l")
            
            p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            processes.append(p)
            
            #Increment the multiplier by one step
            m = increase_multiplier(m)
        
        #Sleep a second and increment the time counter
        sleep(1)
        time_counter += 1
        
    print("\nFinished ramping up.")
    if wait_for_outputs:
        print("Awaiting processes outputs...\n")
        while len(processes) > 0:
            for p in processes:
                #When the process has completed and returned a success exit code, get it's output
                p.wait()
                out, err = p.communicate()
                if out != "" and out is not None: print(str(out))
                if err != "" and err is not None: print(str(err))
                processes.remove(p)

    print("*******************************************************")
    print(str(datetime.now()) + " -- Completed query load generation: ")
    print(output_file + " is being used to populate results (as they come in) and log files")
    print("*******************************************************\n")

# [END run]  

exp_multiplier = 1
processes = []
    
# [START main]
if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('commands_file', help='Delimited file containing the commands to run.')
    parser.add_argument('project_id', help='Project ID to use.', default="nsaad-demos")
    parser.add_argument('time_period', help='Time period to run, specifying the unit (s/m/h) or seconds are assumed')
    parser.add_argument('-r', '--ramp_up_period', help='How often the multiplier should be applied, default set to 10', default=10)
    #TODO: implement the different multiplier options and decide if I want to do exponential and if that makes sense
    parser.add_argument('-m', '--multiplier', help='Define how you want the multiplier to work, valid options are: increment (+1), step2 (+2), exponential(2^)\ndefault is set to incremental\nany incorrect input will be defaulted to incremental', default="incremental")
    parser.add_argument('-mc', '--multiplier-cap', help='Define a cap for how high the multiplier for the number of queries to run can go, default is set to 10', default=10)
    parser.add_argument('-o', '--output-file', help='Name of the file to use to output the log/results.', default=datetime.now().strftime("%Y-%m-%d-%H-%M"))
    parser.add_argument('-nco', '--no_console_output', action='store_true', help='A multiplier to be used to increase the executes of the commands by that multiplier.')
    parser.add_argument('-w', '--wait_for_outputs', action='store_true', help='Should this script wait for outputs from the sub processes or just get the outputs in the output file?')
    parser.add_argument('-l', '--legacy_sql', action='store_true', help='Use legacy sql, default is to use standard sql')

    args = parser.parse_args()
    
    #Setting params global
    global project_id, output_file, multiplier_cap, no_console_output, wait_for_outputs, legacy_sql
    global time_period, ramp_up_period, multiplier
    
    legacy_sql = args.legacy_sql
    project_id = args.project_id
    output_file = args.output_file
    time_period = args.time_period
    wait_for_outputs = args.wait_for_outputs
    
    multiplier = args.multiplier
    
    if args.no_console_output == True:
        no_console_output = "-nco"
    else:
        no_console_output = ""
        
    try:
        multiplier_cap = int(args.multiplier_cap)
        ramp_up_period = int(args.ramp_up_period)
    except: 
        print("ERROR: Make sure you provide numeric values for [multiplier_cap] and [ramp_up_period]\n " + \
              "Run '" + sys.argv[0] + " -h' for details")
        sys.exit();
    
    main(
        args.commands_file)
# [END main]

#TODO FR, any way to change the multiplier to deal with categories too????
#Or have multiple files that I iterate through with one command each , but then I'd be calling multi_queries in parallel from this script