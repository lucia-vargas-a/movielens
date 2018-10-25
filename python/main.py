"""
This main function manages the pipeline to cosume and handle data in neo4j
"""

from python.consumer import Moviliens_Consumer, get_config
import logging

logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', level=logging.DEBUG)
logger = logging.getLogger()


if __name__ == '__main__':

    # Get the configuration parameters from the file
    config_file = get_config()

    # Instance to consume data
    consumer = Moviliens_Consumer(config_file)
    consumer.create_constraints()
    consumer.create_nodes()
    consumer.create_relations()
