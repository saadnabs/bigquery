# Google BigQuery Scripts

Contains scripts for interacting with Google BigQuery

## Setting up your environment

1. Your environment must be setup with [authentication
information](https://developers.google.com/identity/protocols/application-default-credentials#howtheywork). If you're running in your local development environment and you have the [Google Cloud SDK](https://cloud.google.com/sdk/) installed, you can do this easily by running:

        $ gcloud init

2. To use python scripts, Python 2.7.x and python-pip is required :

        $ sudo apt-get install python-pip

3. Ensure the BigQuery API client library for python is up to date :

        $ pip install --upgrade google-api-python-client

4. Install dependencies in `requirements.txt`:

        $ pip install -r requirements.txt

5. TODO Install dependencies for psutil
        git clone https://github.com/giampaolo/psutil/blob/master/INSTALL.rst
        make install-pip
        sudo pip install psutil

## Running the samples

1. Add the statements you want executed into a file that you pass into the command in (2), with the following format:
   - category; number of times to execute; shell/CLI command to execute

2. python multi_queries.py {your-command-file-name} {project-name} {output-file-name} (--no_console_output)

## Running the query load samples

1. Add the statements you want executed into a file that you pass into the command in (2), with the following format:
   - category; number of times to execute; shell/CLI command to execute

2. python query_load_over_time.py {your-command-file-name} {project-name} {time-to-run} (optional: -mc {the upper limit for the multiplier} -o {output-file-name} -m {multiplier increment algorithm} -r {ramp-up-time=period) (--no_console_output) (--wait_for_outputs)