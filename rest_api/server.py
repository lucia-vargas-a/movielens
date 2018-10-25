from flask import Flask
from neomodel import (RelationshipFrom, StringProperty, StructuredNode, UniqueIdProperty)
from grest import GRest, global_config, models, utils
from webargs import fields
import neomodel
import logging
import os

"""RestAPI to expose the moviliens database"""

app = Flask(__name__)

class Movie(StructuredNode, models.Node):
    " Definition of the movie model"
    __validation_rules__ = {
        "movie_id": fields.Str(),
        "title": fields.Str()
    }

    movie_id = UniqueIdProperty()
    title = StringProperty()
    genres = RelationshipFrom("Movie", "HAS_GENRE")


class Genre(StructuredNode, models.Node):
    """Definition of the genre model"""
    genre_id = UniqueIdProperty()
    genre = StringProperty()
    owner = RelationshipFrom("Movie", "HAS_GENRE")


class MovieView(GRest):
    """Movie's view with genres details"""
    __model__ = {"primary": Movie, "secondary": {"Genres": Genre}}
    __selection_field__ = {"primary": "movie_id"}
    route_prefix = "/v1"


def create_app():
    app = Flask(__name__)

    @app.route('/')
    def index():
        return "Moviliens database"

    neomodel.config.DATABASE_URL = global_config.DB_URL
    neomodel.config.AUTO_INSTALL_LABELS = True
    neomodel.config.FORCE_TIMEZONE = True

    if global_config.LOG_ENABLED:
        logging.basicConfig(filename=os.path.abspath(os.path.join(
            global_config.LOG_LOCATION, global_config.LOG_FILENAME)), format=global_config.LOG_FORMAT)
        app.ext_logger = logging.getLogger()
        app.ext_logger.setLevel(global_config.LOG_LEVEL)
        handler = logging.handlers.RotatingFileHandler(
            os.path.abspath(os.path.join(global_config.LOG_LOCATION, global_config.LOG_FILENAME)),
            maxBytes=global_config.LOG_MAX_BYTES,
            backupCount=global_config.LOG_BACKUP_COUNT)
        app.ext_logger.addHandler(handler)
    else:
        app.ext_logger = app.logger

    MovieView.register(app, route_base="/movies", trailing_slash=False)
    return app


if __name__ == '__main__':
    app = create_app()
    app.run(debug=True, threaded=True)
