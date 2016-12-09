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
    global output_file, output_file_only
    output_file = js_path + filename + "-dataloader.js"
    output_file_only = filename + "-dataloader.js"
    
    print("output_file " + str(output_file))
    
    #Remove the dashes from the default date if not file name is provided as it's not a valid JS variable name, and add results_ at the start
    js_var_name = filename.replace("-", "")
    if is_int(js_var_name) == True:
        js_var_name = "results_" + js_var_name
        
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
                
                '''
                if count < 1: #continue #test purposes
                    print(values)
                    print("len: " + str(len(values)))
                    count += 1
                '''
                
                status = values[0]
                bq_duration = values[1]
                bash_duration = values[2]
                bytes_processed = values[3]
                bash_start_time = datetime.strptime(values[4], "%Y-%m-%d %H:%M:%S.%f")
                bash_end_time = datetime.strptime(values[5], "%Y-%m-%d %H:%M:%S.%f")
                bq_creation_time = datetime.strptime(values[6], "%Y-%m-%d %H:%M:%S.%f")
                bq_start_time = datetime.strptime(values[7], "%Y-%m-%d %H:%M:%S.%f")
                bq_end_time = datetime.strptime(values[8], "%Y-%m-%d %H:%M:%S.%f")
                category = values[9]
                query = values[10]
                job_id = values[11]
                run_id = values[12]
                error_result = values[13]
                
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
                
                f.write("        [\"" + str(job_id) + "\", \"Bash\", new Date(0, 0, 0, " + str(bash_start_hour) + ", " + str(bash_start_minute) + ", " + str(bash_start_second) + ")," + \
                        " new Date(0, 0, 0, " + str(bash_end_hour) + ", " + str(bash_end_minute) + ", " + str(bash_end_second) + ")], \n")
                f.write("        [\"" + str(job_id) + "\", \"BQ\", new Date(0, 0, 0, " + str(bq_start_hour) + ", " + str(bq_start_minute) + ", " + str(bq_start_second) + ")," + \
                        " new Date(0, 0, 0, " + str(bq_end_hour) + ", " + str(bq_end_minute) + ", " + str(bq_end_second) + ")], \n")
            
            f.write("    ];\n")
            f.write("  listOfResults[\"" + js_var_name + "\"] = " + js_var_name + ";")
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
            output.write('<script type="text/javascript" src="js/' + output_file_only + '"></script>\n')
            found_placeholder = True
        else:
            print("shouldn't hit other condition...")
    
def is_int(s):
    try: 
        int(s)
        return True
    except ValueError:
        return False

def main(resultsFile):
    writeFile(resultsFile)
    update_results_viewer()
    
results_viewer_template = "results-viewer-template.html"
results_viewer_output = "runs/results/results-viewer.html"
results_viewer_temp = "results-viewer-temp.html"

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('resultsFile', help='File containing results to create datafile for timeline chart page.')
    
    args = parser.parse_args()
    main(args.resultsFile)