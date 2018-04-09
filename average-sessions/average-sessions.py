from google.cloud import pubsub
from google.cloud.pubsub import types
from google.cloud import bigtable
from google.oauth2 import service_account
from google.cloud import bigquery
import uuid

import sys
import time

class AverageSessions:

    start_key_suffix = "00000"
    end_key_suffix  = "99999"
    
	# Instantiates a client
	bigquery_client = bigquery.Client()
	
	# The name for the new dataset
	dataset_id = 'session_averages'
	
	# Prepares a reference to the new dataset
	dataset_ref = bigquery_client.dataset(dataset_id)
	dataset = bigquery.Dataset(dataset_ref)
	
	# Creates the new dataset
	dataset = bigquery_client.create_dataset(dataset)


    def __init__(self, credentials, subscription_name, bt_instance_name, bt_table_name ):

        self.bigtable_client = bigtable.Client(credentials = credentials, read_only=True)
        self.bigtable_instance = self.bigtable_client.instance(bt_instance_name)
        self.bigtable_table = self.bigtable_instance.table(bt_table_name)

        self.subscriber = pubsub.SubscriberClient(credentials = credentials)
        self.subscription = self.subscriber.subscribe(subscription_name, callback = self.pubsub_callback)

    def pubsub_callback(self, message):
        session_id = int(bytes.decode(message.data))
        self.read_bigtable_rows(session_id)
        message.ack()

#    def writeToBQ():
#	        rows_to_insert = [
#	            (session_id, join),
#	        ]   
	
	def stream_data(self, table, data, schema):
	    # first checks if table already exists. If it doesn't, then create it
	    r = self.service.tables().list(projectId=bigtable-sessionize,
	                                     datasetId=session_averages).execute()
	    table_exists = [row['tableReference']['tableId'] for row in
	                    r['tables'] if
	                    row['tableReference']['tableId'] == table]
	    if not table_exists:
	        body = {
	            'tableReference': {
	                'tableId': table,
	                'projectId': your_project_id,
	                'datasetId': your_dataset_id
	            },
	            'schema': schema
	        }
	        self.service.tables().insert(projectId=your_project_id,
	                                     datasetId=your_dataset_id,
	                                     body=body).execute()
	
	    # with table created, now we can stream the data
	    # to do so we'll use the tabledata().insertall() function.
	    body = {
	        'rows': [
	            {
	                'json': data,
	                'insertId': str(uuid.uuid4())
	            }
	        ]
	    }
	    self.service.tabledata().insertAll(projectId=your_project_id),
	                                       datasetId=your_dataset_id,
	                                       tableId=table,
	                                         body=body).execute(num_retries=5)
	
    def read_bigtable_rows(self, session_id):
        start_key ="{}#{}".format(session_id, self.start_key_suffix).encode('UTF-8')
        end_key = "{}#{}".format(session_id, self.end_key_suffix).encode('UTF-8')
        row_data = self.bigtable_table.read_rows(
            start_key=start_key,
            end_key=end_key)
        row_data.consume_all()
        rows = row_data.rows
#        join = format(session_id, sum/float(num_messages))
        sum = 0
        num_messages = 0
        for rowkey, row in rows.items():
            cell = row.cells['data'][b'value'][0]
            value = int(bytes.decode(cell.value))
            sum = sum + value
            num_messages = num_messages + 1
            
        if num_messages > 0:
          print("Average for Session: {} = {}".format(session_id, sum/float(num_messages)))
#          writeToBQ()
        

if __name__ == "__main__":

    credentials = service_account.Credentials.from_service_account_file(
        '../credentials/average-sessions.json').with_scopes(
        ['https://www.googleapis.com/auth/cloud-platform'])

    subscription_name = 'projects/bigtable-sessionize/subscriptions/average-sessions'
    bt_instance_name = 'messages'
    bt_table_name = 'messages'

    average_sessions = AverageSessions(credentials, subscription_name, bt_instance_name, bt_table_name)

    while True:
        time.sleep(60)