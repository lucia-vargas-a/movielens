CREATE
  (`0` :Movie_Database {database_id:'int',database_name:'string'}) ,
  (`1` :Movie {movie_id:'int',title:'string',year:'int'}) ,
  (`2` :Tag {tag_id:'integer',tag_name:'string'}) ,
  (`5` :User {user_id:'integer'}) ,
  (`6` :Genre {genre_name:'string'}) ,
  (`0`)-[:`STORED` ]->(`1`),
  (`5`)-[:`RATED` {timestamp:'int',score:'float'}]->(`1`),
  (`1`)-[:`HAS` ]->(`6`),
  (`1`)-[:`HAS` {Relevance:'integer'}]->(`2`)
