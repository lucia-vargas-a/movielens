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

# dataset files
movie_path = "'file:///%s'" % self.config['files']['movie']
rating_path = "'file:///%s'" % self.config['files']['rating']
tag_path = "'file:///%s'" % self.config['files']['tag']
links_path = "'file:///%s'" % self.config['files']['links']
genome_tags_path = "'file:///%s'" % self.config['files']['genome_tags']
genome_scores_path = "'file:///%s'" % self.config['files']['genome_scores']

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

    def create_nodes(self):
        """
            Function to read the dataset and create the nodes in the database
        """
        try:
            # Load Movie nodes
            query = """
            USING PERIODIC COMMIT
            LOAD CSV WITH HEADERS FROM %s AS mline
            WITH mline LIMIT 10
            MERGE (movie:Movie {movie_id: ToInt(mline.movieId)})
            ON CREATE SET movie.title = mline.title
            WITH split(mline.genres, "|") AS genre
            UNWIND genre as g
            MERGE (gen:Genre {genre_name:g})
            MERGE (movie)-[:IN_GENRE]->(gen);
            ;""" % movie_path
            graph.run(query)

            #load movie databases IDs
            query = """
            USING PERIODIC COMMIT
            LOAD CSV WITH HEADERS
            FROM %s AS line
            WITH line LIMIT 10
            MATCH (movie:Movie {movie_id: line.movieId})
            SET movie.imdb_id = line.imdbId,
                movie.tmdb_id = line.tmdbId;
            ;""" % links_path
            graph.run(query)

            #load genome tags
            query = """
            USING PERIODIC COMMIT
            LOAD CSV WITH HEADERS
            FROM %s AS tline
            WITH tline LIMIT 10
            MERGE(tag:Tag {tag_id: tline.tagId})
            SET tag.tag_id = tline.tag;
            ;""" % genome_tags_path
            graph.run(query)

            # Load ratings
            """USING PERIODIC COMMIT
            LOAD CSV WITH HEADERS
            FROM %s as rline
            MERGE (user:User {userId:rline.userId})
            WITH rline LIMIT 5
            MATCH (movie:Movie {movieId:rline.movieId})
            CREATE (user)-[r:RATED]->(movie)
            SET r.rating = toFloat(rline.rating),
                r.timestamp = ToInt(rline.timestamp);
            """ % rating_path

            # Load movie-user tags
            query = """
            USING PERIODIC COMMIT
            LOAD CSV WITH HEADERS
            FROM %s AS tline
            MATCH (user:User{user_id:tline.userId})
            MATCH (movie:Movie{movie_id:tline.movieId})
            CREATE (user)-[a:TAGGED]->(movie)
            SET a.timestamp = tline.timestamp;
            ;""" % tag_path
            graph.run(query)

            # Load Tag nodes
            query = """
            USING PERIODIC COMMIT
            LOAD CSV WITH HEADERS
            FROM %s AS mline
            WITH SPLIT(mline.genre,'|') as genre
            MERGE (genre:Genre {genre_name:genre})
            ;""" % movie_path
            graph.run(query)

        except ValueError:
            return None

    def create_constraints(self):
        try:
            # create constraints for unique propoerties
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
            return None

    def create_relations(self):

        # Movie > Tag relation
        query = """
        USING PERIODIC COMMIT
        LOAD CSV WITH HEADERS
        FROM %s AS rline
        CREATE (user)-[HAS]->(genre)
        ;""" % rating_path
        graph.run(query)

        # User > Rating relation
        query = """
        USING PERIODIC COMMIT
        LOAD CSV WITH HEADERS
        FROM %s AS rline
        CREATE (user)-[r:RATED]->(movie)
        SET r.rating = rline.rating, r.timestamp = TOINT(rline.genre)
        ;""" % rating_path