import argparse
import datetime
import math
import subprocess
import sys  

from datetime import datetime
from time import sleep

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
        current_multiplier =  math.pow(2, exp_multiplier) #1, 2, 4, 8, 16
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
            print("At " + str(time_counter) + " second(s) hit ramp up cycle " + str((time_counter / ramp_up_period) + 1) + " setting multiplier to " + str(int(m)))
            
            #TODO test, don't block on call... need to do it async and move on... otherwise need to create new processes here too with popen.. or threadpool
            #TODO test, what happens to output here?
            #p = subprocess.Popen(["python","multi_queries.py", commands_file, project_id, output_file, str(m)], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            subprocess.call(["python","multi_queries.py", commands_file, project_id, output_file, str(m)])
            
            #For testing initially
            #print('subprocess.call(["python","multi_queries.py", ' + commands_file + ', ' + project_id + ', ' + output_file + ', ' + str(int(m)) + '])')
            
            #Increment the multiplier by one
            m = increase_multiplier(m)
        
        #Sleep a second and increment the time counter
        sleep(1)
        time_counter += 1
    
    print("*******************************************************")
    print(str(datetime.now()) + " -- Completed query load generation: ")
    print("\n*******************************************************\n")

# [END run]  

exp_multiplier = 1
    
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
                                                    
                                                    #TODO test, I think this is sorted -  format the date with less milliseconds and no spaces
    parser.add_argument('-o', '--output-file', help='Name of the file to use to output the log/results.', default=datetime.now().strftime("%Y-%m-%d-%H-%M"))
    #TODO change some arguments to flag for "no_console_output", multiplier cap (use 5 as default), multiplier (use increment as default), ramp_up_period (use 10 as default)

    args = parser.parse_args()
    
    #Setting params global
    global project_id, output_file, multiplier_cap
    global time_period, ramp_up_period, multiplier
    
    project_id = args.project_id
    output_file = args.output_file
    time_period = args.time_period
    
    multiplier = args.multiplier
    
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