from google.cloud import pubsub
from google.cloud.pubsub import types
from google.oauth2 import service_account

from random import randint
import json
import time
import os
import logging
import sys
import argparse
from multiprocessing import Pool

class GenerateMesssages:

    def __init__(self, client, topic ):
        logging.info("banana")
        self.topic = topic
        self.client = client

    def generate_message(self):

        while True:
            session_id = randint(1, 999999 )
            num_messages = randint(1, 10)
            for message_num in range(1, num_messages):
                message = {}
                message['session_id'] = session_id
                message['value'] = randint(1,100)
                message['message_num'] = message_num
                data = json.dumps(message)
                logging.info('Sending message: {}'.format(data))
                # Data must be a bytestring
                data = data.encode('utf-8')
                self.client.publish(self.topic, data=data,
                    timestamp_ms=str(int(time.time()*1000)),
                    message_id='{}{}'.format(session_id, message_num))
                if randint(1,10) == 10:
                    time.sleep(80)
                else:
                    time.sleep(randint(1, 5))

if __name__ == "__main__":
    logging.info("apple")
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    client = pubsub.PublisherClient(
        batch_settings=types.BatchSettings(max_messages=500))
    topic = 'projects/bigtable-sessionize/topics/messages'

    generate_messages = GenerateMesssages(client, topic)

    generate_messages.generate_message()

