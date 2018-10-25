"""
This main function manages the pipeline to cosume and handle data in neo4j
"""

from consumer import Movielens_Consumer, get_config
import logging
import time

logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', level=logging.DEBUG)
logger = logging.getLogger()


if __name__ == '__main__':
    start = time.strftime("%Y-%m-%d %H:%M:%s", time.localtime(time.time()))
    logger.info('Process start at %s', str(start))

    # Get the configuration parameters from the file
    config_file = get_config()

    # Instance to consume data
    consumer = Movielens_Consumer(config_file)

    # Create the indexes required before loading data into neo4j
    constraints_start = time.time()
    consumer.create_constraints()
    logger.info("Constraints created.\nThe process took {0} seconds\n\n".format(time.time() - constraints_start))

    # Load dataset into neo4j database: nodes, relations and attributes
    consume_start = time.time()
    consumer.create_from_dataset()
    logger.info("Constraints created.\nThe process took {0} seconds\n\n".format(time.time() - consume_start))
