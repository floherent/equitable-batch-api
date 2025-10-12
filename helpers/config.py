import logging
import os

import cspark.sdk as Spark

CONFIG_DIR = os.path.dirname(os.path.abspath(__file__))
METADATA = {
    'min_runners': 10,  # initial_workers
    'max_runners': 100,  # max_workers
    'chunks_per_vm': 1,  # chunks_per_request
    'runners_per_vm': 2,  # runner_thread_count
    'max_input_size': 40 * 1024 * 1024,  # 40 MB
    'max_output_size': 50 * 1024 * 1024,  # 50 MB
    'accuracy': 1.0,  # zero error tolerance
}

logging.basicConfig(filename='console.log', filemode='w', format=Spark.DEFAULT_LOGGER_FORMAT)
logger = Spark.get_logger(context='Equitable')


class Config:
    NUM_CHUNK = 18  # total records = 18 * 48 = 864
    CHUNK_SIZE = 48
    TIMEOUT = 120_000
    SERVICE_URI = 'Equitable Life CA/Optimize_UpdateIndexFunctions_Tee'
    INPUT_DIR = os.path.abspath(os.path.join(CONFIG_DIR, '..', 'inputs'))
    OUTPUT_DIR = os.path.abspath(os.path.join(CONFIG_DIR, '..', 'outputs'))

    # Post processing parameters
    AGGREGATE_FILE = os.path.abspath(os.path.join(CONFIG_DIR, '..', 'assets', 'results.csv')),
    AGGREGATE_KEYS = ['YEAR', 'DB TOP-UPS', 'MB TOP-UPS']
