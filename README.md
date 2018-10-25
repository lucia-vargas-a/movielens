### Moviliens dataset analysis

Thank you for reviewing this project!!

The goal is to to analyze the dataset of moviliens by designing an effective graph structure and data pipeline.


#### Project components

1. An effective graph structure out of the dataset
    The graph was build using the Arrows tool for Neo4j graph databases
    
    [Graph design](docs/graph_design.md)

2. The design of a data pipeline to ingest the data into the graph database

3. An API to retrieve individual node in the graph as well as functionality to search the graph
and retrieve the results

**Bonus:**

4. A unitest to validate that a dataset of movies was loaded completely

    [unitest code](python/test.py)

#### Data pipeline code details

##### Dataset source files
Defined as parameters in order to allow flexibility without affecting the code.
- The configuration file can be found [here](config/movielens_config.json)
- The code to read the configuration can be found in the function [get_config()](python/consumer.py)

##### Dataset consumption
Built in the class [Moviliens_Consumer](python/consumer.py) which contains 3 functions:
- __init__ to initialize the class components
- **create_constraints()** to build the constraints required before loading data into the graph database
- **create_from_dataset** to consume the dataset from the csv files
3. The main function to consume the data can be found [here](python/main.py)

#### Selected languages
- The graph database is implemented in **Neo4j**
- The code to consume the data is written in **Python 3**
- The repository is built on Github
- The container is built using Docker



>**author: Lucía Vargas**    
[linkedin](https://www.linkedin.com/in/lucia-vargasa/)