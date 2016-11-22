import argparse
import datetime
import sys

from datetime import datetime

def writeFile(resultsFile):
    
    print("resultsFile: " + resultsFile)
    #runs/results/newtest-results.csv
    to_dash = resultsFile.rfind("-")
    if to_dash == -1:
        print("file doesn't match {myfile}-results.csv format, exiting")
        sys.exit()
        
    print("to_dash " + str(to_dash))
    #runs/results/newtest
    output_file = resultsFile[:to_dash] + "-dataloader.js"
    print("output_file " + str(output_file))
    #runs/results/newtest-dataloader.js
    
    to_slash = output_file.rfind("/") + 1
    js_var_name = resultsFile[to_slash:to_dash]
    print("js_var_name " + str(js_var_name))
    #newtest
    
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

def main(resultsFile):
    writeFile(resultsFile)
    #TODO recreate the results-viewer, while appending the JS file

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('resultsFile', help='File containing results to create datafile for timeline chart page.')
    
    args = parser.parse_args()
    main(args.resultsFile)