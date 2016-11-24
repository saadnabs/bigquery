import argparse
import datetime
import os
import shutil
import sys

from datetime import datetime
from shutil import move

def writeFile(resultsFile):
    
    print("resultsFile: " + resultsFile)
    #runs/results/newtest-results.csv
    
    #Find the dash
    to_dash = resultsFile.rfind("-")
    if to_dash == -1:
        print("file doesn't match {myfile}-results.csv format, exiting")
        sys.exit()
        
    print("resultsFile " + str(resultsFile))
    #Find the slash
    to_slash = resultsFile.rfind("/") + 1
    
    #Extract just the file name
    filename = resultsFile[to_slash:to_dash]
    
    #Extract the path
    path = resultsFile[:to_slash]
    
    #Create the JS path
    js_path = path + "js/"
    
    #Create the final output file for the JS
    global output_file
    output_file = js_path + filename + "-dataloader.js"
    
    print("output_file " + str(output_file))
    
    js_var_name = filename
    if not os.path.exists(js_path):
        os.makedirs(js_path)
        
    
    
    '''if not os.path.exists(result_path):
        os.makedirs(result_path)
    '''
    
    try:
        #Read the results file
        rf = open(resultsFile, 'r')

        #Dump the header row
        rf.readline()
        
        try: 
        
            f = open(output_file, 'wt')
            f.write("var " + js_var_name + " = [\n\n")
            
            count = 0 #test purposes
            for line in rf: 
                values = line.split(';')
                
                bq_duration = values[1]
                bash_duration = values[2]
                bytes_processed = values[3]
                bash_start_time = datetime.strptime(values[4], "%Y-%m-%d %H:%M:%S.%f")
                bash_end_time = datetime.strptime(values[5], "%Y-%m-%d %H:%M:%S.%f")
                bq_start_time = datetime.strptime(values[6], "%Y-%m-%d %H:%M:%S.%f")
                bq_end_time = datetime.strptime(values[7], "%Y-%m-%d %H:%M:%S.%f")
                category = values[8]
                query = values[9]
                job_id = values[10]
                run_id = values[11]
                
                bash_start_hour = bash_start_time.hour
                bash_start_minute = bash_start_time.minute
                bash_start_second = bash_start_time.second
                
                bash_end_hour = bash_end_time.hour
                bash_end_minute = bash_end_time.minute
                bash_end_second = bash_end_time.second
                
                bq_start_hour = bq_start_time.hour
                bq_start_minute = bq_start_time.minute
                bq_start_second = bq_start_time.second
                
                bq_end_hour = bq_end_time.hour
                bq_end_minute = bq_end_time.minute
                bq_end_second = bq_end_time.second
                
                f.write("        ['" + str(job_id) + "', 'Bash', new Date(0, 0, 0, " + str(bash_start_hour) + ", " + str(bash_start_minute) + ", " + str(bash_start_second) + ")," + \
                        " new Date(0, 0, 0, " + str(bash_end_hour) + ", " + str(bash_end_minute) + ", " + str(bash_end_second) + ")], \n")
                f.write("        ['" + str(job_id) + "', 'BQ', new Date(0, 0, 0, " + str(bq_start_hour) + ", " + str(bq_start_minute) + ", " + str(bq_start_second) + ")," + \
                        " new Date(0, 0, 0, " + str(bq_end_hour) + ", " + str(bq_end_minute) + ", " + str(bq_end_second) + ")], \n")
            
                if count > 1: continue #test purposes
            
            f.write("    ];\n")
            f.write("  listOfResults['" + js_var_name + "'] = " + js_var_name + ";")
        except IOError:
            output_log("Can not open dataloader file to write to, check the script's permissions in this directory", "true", 40)
            f.close()
        finally:
            f.close()

    except IOError:
        output_log("Can not open results file to read, check the script's permissions in this directory", "true", 40)
        rf.close()
        
    finally:
        rf.close()

def update_results_viewer():
    try:
        #Check if output file already exists
        output = open(results_viewer_output, 'r')
        
        try:
            #Create a temp file
            temp = open(results_viewer_temp, 'w')
            
            #If so, call make copy and append with existing output
            create_output_append_js(output, temp)
            
            #Copy temp to output filename
            os.remove(results_viewer_output)
            #TODO move to /runs/results/
            move(results_viewer_temp, results_viewer_output)
        except IOError:
            print("Could not create temp file, failing the write...")
            sys.exit()
            
        finally:
            temp.close()
            
    except IOError:
            #Output file didn't exist, opening template
            try:
                template = open(results_viewer_template, 'r')
                try:
                    output = open(results_viewer_output, 'w')
                    create_output_append_js(template, output)
                    #TODO move to /runs/results/
                except IOError:
                    print("Can not open output file, failing the write...")
                    sys.exit()
                    output.close()
                finally:
                    output.close()
        
            except IOError:
                print("Can not open template file, failing the write...")
                sys.exit()
                template.close()
            finally:
                template.close()
    finally:
        output.close()
        
def create_output_append_js(original, output):
    
    found_placeholder = False
    for line in original.readlines():
        if line.find("python_js_below") == -1 or found_placeholder == True:
            output.write(line)
        elif found_placeholder == False and line.find("python_js_below") != -1:
            #Output the placeholder file
            output.write(line)
            
            #TODO FR: would be nice to not insert if this source already exists, but not required
            #Then output the new line
            output.write('<script type="text/javascript" src="' + output_file + '"></script>\n')
            found_placeholder = True
        else:
            print("shouldn't hit other condition...")
    

def main(resultsFile):
    writeFile(resultsFile)
    update_results_viewer()
    #TODO recreate the results-viewer, while appending the JS file
    
results_viewer_template = "results-viewer-template.html"
results_viewer_output = "results-viewer.html"
results_viewer_temp = "results-viewer-temp.html"

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('resultsFile', help='File containing results to create datafile for timeline chart page.')
    
    args = parser.parse_args()
    main(args.resultsFile)