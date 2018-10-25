import os
import json
import logging
import csv
from py2neo import Graph, Database


# Default location of the configuration and dataset files
DEFAULT_CONFIG_PATH = 'config//movielens_config.json'
DEFAULT_DATASET_PATH = 'dataset/movies.csv'

# Define graph database objects
db = Database("bolt://localhost:7687")
url = "bolt://localhost:7687"
graph = Graph(url)

# Log the processes
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()

def get_config():
    """
    Function to obtain the main parameters of source dataset and connections to the database from a configuration file
    """
    dir_name, running_filename = os.path.split(os.path.abspath(__file__))
    file_dir = os.path.dirname(dir_name)
    config_file_name = DEFAULT_CONFIG_PATH
    config_path = os.path.join(file_dir, config_file_name)

    with open(config_path) as config_file:
        config = json.load(config_file)
    return config

class Moviliens_Consumer(object):
    """This class manages the pipeline to consume the dataset and store the data in the graph database neo4j"""

    def __init__(self, config_file):
        self.config = config_file

        # read the dataset file names form the configuration file
        self.movies_path = "'file:///%s'" % self.config['files']['movie']
        self.rating_path = "'file:///%s'" % self.config['files']['rating']
        self.tag_path = "'file:///%s'" % self.config['files']['tag']
        self.links_path = "'file:///%s'" % self.config['files']['links']
        self.genome_tags_path = "'file:///%s'" % self.config['files']['genome_tags']
        self.genome_scores_path = "'file:///%s'" % self.config['files']['genome_scores']

    def create_constraints(self):
        try:
            # Create constraints for unique propoerties
            query_unique = """CREATE CONSTRAINT ON (u:User) ASSERT u.user_id IS UNIQUE;"""
            graph.run(query_unique)
            query_unique = """CREATE CONSTRAINT ON (m:Movie) ASSERT m.movie_id IS UNIQUE;"""
            graph.run(query_unique)
            query_unique = """CREATE CONSTRAINT ON (g:Genre) ASSERT g.genre_name IS UNIQUE;"""
            graph.run(query_unique)
            query_unique = """CREATE CONSTRAINT ON (t:Tag) ASSERT t.database_id IS UNIQUE;"""
            graph.run(query_unique)
            query_unique = """CREATE CONSTRAINT ON (d:Database) ASSERT d.database_id IS UNIQUE;"""
            graph.run(query_unique)
        except ValueError:
            logger.exception("message")
            return None

    def create_from_dataset(self):
        """
            Function to read the full dataset and create the nodes, relations and attributes in the database
        """
        try:
            # Load Movie nodes
            query = """
            USING PERIODIC COMMIT
            LOAD CSV WITH HEADERS FROM %s AS mline
            WITH mline
            MERGE (movie:Movie {movie_id:ToInt(mline.movieId)})
            ON CREATE SET movie.title = mline.title
            WITH split(mline.genres, "|") AS genre
            UNWIND genre as g
            MERGE (gen:Genre {genre_name:g})
            MERGE (movie)-[:HAS_GENRE]->(gen)
            ;""" % self.movies_path
            graph.run(query)

            #Load movie databases IDs
            query = """
            USING PERIODIC COMMIT
            LOAD CSV WITH HEADERS
            FROM %s AS line
            WITH line
            MATCH (movie:Movie {movie_id: line.movieId})
            SET movie.imdb_id = line.imdbId,
                movie.tmdb_id = line.tmdbId
            ;""" % self.links_path
            graph.run(query)

            #Load movie tags
            query = """
            USING PERIODIC COMMIT
            LOAD CSV WITH HEADERS
            FROM %s AS xline
            MATCH (movie:Movie {movie_id:xline.movieId})
            WITH xline
            MERGE (tag:Tag{tag_id:xline.tagId})
            MERGE (movie)-[h:HAS_TAG]->(tag)
            SET h.relevance = xline.relevance
            ;""" % self.genome_scores_path
            graph.run(query)

            # Load ratings
            """USING PERIODIC COMMIT
            LOAD CSV WITH HEADERS
            FROM %s as rline
            MERGE (user:User {userId:rline.userId})
            WITH rline
            MATCH (movie:Movie {movieId:rline.movieId})
            MERGE (user)-[r:RATED]->(movie)
            SET r.rating = toFloat(rline.rating),
                r.timestamp = ToInt(rline.timestamp)
            ;""" % self.rating_path

            # Load movie-user tags
            query = """
            USING PERIODIC COMMIT
            LOAD CSV WITH HEADERS
            FROM %s AS tline
            MERGE (user:User{user_id:tline.userId})
            WITH tline
            MATCH (movie:Movie{movie_id:tline.movieId})
            CREATE (user)-[a:TAGGED]->(movie)
            SET a.timestamp = tline.timestamp,
                a.tag = tline.tag
            ;""" % self.tag_path
            graph.run(query)

        except ValueError:
            logger.exception("message")
            return None
