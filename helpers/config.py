import logging
import os

import cspark.sdk as Spark

CONFIG_DIR = os.path.dirname(os.path.abspath(__file__))
METADATA = {
    'min_runners': 10,  # initial_workers
    'max_runners': 500,  # max_workers
    'chunks_per_vm': 1,  # chunks_per_request
    'runners_per_vm': 2,  # runner_thread_count
    'accuracy': 1.0,  # zero error tolerance
}

logging.basicConfig(filename='console.log', filemode='w', format=Spark.DEFAULT_LOGGER_FORMAT)
logger = Spark.get_logger(context='Equitable')


class Config:
    NUM_CHUNK = 12
    CHUNK_SIZE = 72
    TIMEOUT = 120_000
    SERVICE_URI = 'Equitable/DB_MB_Topup_Calculator'
    INPUT_DIR = os.path.abspath(os.path.join(CONFIG_DIR, '..', 'inputs'))
    OUTPUT_DIR = os.path.abspath(os.path.join(CONFIG_DIR, '..', 'outputs'))
