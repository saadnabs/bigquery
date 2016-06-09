# Google BigQuery Scripts

Contains scripts for interacting with Google BigQuery

## Running the samples

1. Your environment must be setup with [authentication
information](https://developers.google.com/identity/protocols/application-default-credentials#howtheywork). If you're running in your local development environment and you have the [Google Cloud SDK](https://cloud.google.com/sdk/) installed, you can do this easily by running:

        $ gcloud init

2. To use python scripts, Python 2.7.x and python-pip is required :

        $ sudo apt-get install python-pip

3. Ensure the BigQuery API client library for python is up to date :

        $ pip install --upgrade google-api-python-client

4. Install dependencies in `requirements.txt`:

        $ pip install -r requirements.txt

