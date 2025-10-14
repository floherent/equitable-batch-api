import os
import signal
import sys
import time

import cspark.sdk as Spark

from .chunk import ChunkGenerator, ChunkProcessor
from .config import METADATA, Config, logger
from .threads import run_threads


def process_chunks(pipeline: Spark.Pipeline, processor: ChunkProcessor):
    """Processes the CSV chunks using a single ChunkGenerator for the input directory."""
    start_time = time.time()
    generator = ChunkGenerator(Config.INPUT_DIR)
    run_threads(
        pipeline=pipeline,
        chunk_generator=generator,
        chunk_processor=processor,
        threads_up=1,
        threads_down=1,
        target=0.9,
        delay=1.5,
        verbose=True,
        logging=False,
    )
    logger.debug('Done processing all records')

    if not processor.is_empty:
        processor.save()

    return time.time() - start_time


def run_batch():
    """Runs the batch processing logic."""
    pipeline = None

    try:
        with Spark.Client(logger={'context': logger.name}, timeout=Config.TIMEOUT).batches as batches:
            batch = batches.create(Config.SERVICE_URI, **METADATA).data
            pipeline = batches.of(batch['id'])
            signal.signal(signal.SIGINT, lambda s, f: handle_interrupt(s, f, pipeline))

        processor = ChunkProcessor(True, Config.OUTPUT_DIR)
        elapsed_time = process_chunks(pipeline, processor)
        pipeline.dispose()

        status = pipeline.get_status().data
        logger.debug(f'{status["records_completed"] / elapsed_time} records per second (on average)')
    except Exception as exc:
        logger.error(exc)
        if pipeline and pipeline.state == 'open':
            logger.debug('Canceling the pipeline due to an error...')
            pipeline.cancel()
    finally:
        if pipeline:
            if pipeline.state == 'open':
                pipeline.dispose()
            pipeline.close()


def handle_interrupt(signal, frame, pipeline: Spark.Pipeline):  # noqa: ARG001
    """Handles the Ctrl + C (SIGINT) signal."""
    logger.debug('\nCtrl + C detected. Attempting to cancel the pipeline...')
    if pipeline:
        pipeline.cancel()
        logger.debug('Pipeline canceled.')
    sys.exit(0)
