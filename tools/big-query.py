from bigquery import get_client

# BigQuery project id as listed in the Google Developers Console.
project_id = 'project_id'

# Service account email address as listed in the Google Developers Console.
service_account = 'my_id_123@developer.gserviceaccount.com'

# PKCS12 or PEM key provided by Google.
key = 'key.pem'

client = get_client(project_id, service_account=service_account,
                    private_key_file=key, readonly=True)

# Submit an async query.
job_id, _results = client.query('SELECT * FROM dataset.my_table LIMIT 1000')

# Check if the query has finished running.
complete, row_count = client.check_job(job_id)

# Retrieve the results.
results = client.get_query_rows(job_id)