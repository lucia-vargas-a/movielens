LOAD CSV WITH HEADERS FROM "file:///movies.csv" AS row
RETURN count(1);

LOAD CSV WITH HEADERS FROM "file:///genome_tags.csv" AS row
RETURN count(1);

LOAD CSV WITH HEADERS FROM "file:///genome_scores.csv" AS row
RETURN count(1);

LOAD CSV WITH HEADERS FROM "file:///links.csv" AS row
RETURN count(1);

LOAD CSV WITH HEADERS FROM "file:///ratings.csv" AS row
RETURN count(1);

LOAD CSV WITH HEADERS FROM "file:///tags.csv" AS row
RETURN count(1);
