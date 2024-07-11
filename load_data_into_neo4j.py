# Load datasets
import pandas as pd
from py2neo import Graph, Node, Relationship
from neo4j import GraphDatabase


# Load MovieLens data
u_data = pd.read_csv('movielens/u.data', sep='\t', names=['user_id', 'item_id', 'rating', 'timestamp'])
u_item = pd.read_csv('movielens/u.item', sep='|', encoding='ISO-8859-1', names=['movie_id', 'movie_title', 'release_date', 'video_release_date', 'IMDb_URL', 'unknown', 'Action', 'Adventure', 'Animation', 'Children', 'Comedy', 'Crime', 'Documentary', 'Drama', 'Fantasy', 'Film-Noir', 'Horror', 'Musical', 'Mystery', 'Romance', 'Sci-Fi', 'Thriller', 'War', 'Western'])

# Load MovieLens data
u_data = pd.read_csv('movielens/u.data', sep='\t', names=['user_id', 'item_id', 'rating', 'timestamp'])
u_item = pd.read_csv('path_to_ml-100k/u.item', sep='|', encoding='ISO-8859-1', names=['movie_id', 'movie_title', 'release_date', 'video_release_date', 'IMDb_URL', 'unknown', 'Action', 'Adventure', 'Animation', 'Children', 'Comedy', 'Crime', 'Documentary', 'Drama', 'Fantasy', 'Film-Noir', 'Horror', 'Musical', 'Mystery', 'Romance', 'Sci-Fi', 'Thriller', 'War', 'Western'])
u_user = pd.read_csv('path_to_ml-100k/u.user', sep='|', names=['user_id', 'age', 'gender', 'occupation', 'zip_code'])



# Load IMDb data
title_basics = pd.read_csv('IMDB/title.basics.tsv', sep='\t', na_values='\\N', dtype={'tconst': str, 'primaryTitle': str, 'genres': str})
title_ratings = pd.read_csv('IMDB/title.ratings.tsv', sep='\t', na_values='\\N', dtype={'tconst': str})

# Load mapping file
mapping_data = pd.read_csv('combined_dataset/matched_title.pd', sep='\t', names=['ml_id', 'imdb_id'])


# Connect to Neo4j
graph = Graph("neo4j+s://52baf6d0.databases.neo4j.io", auth=("neo4j", "zQV7pyc9bNYBgfjH3G6mh9WoL36cVO_0h2kOFxLr6Bw"))  # Adjust connection details accordingly

# verification
URI = "neo4j+s://52baf6d0.databases.neo4j.io"
AUTH = ("neo4j", "zQV7pyc9bNYBgfjH3G6mh9WoL36cVO_0h2kOFxLr6Bw")
with GraphDatabase.driver(URI, auth=AUTH) as driver:
    driver.verify_connectivity()
    print("Connection established.")



# Create MovieLens nodes
# Create MovieLens nodes with genres as a list
for index, row in u_item.iterrows():
    genres = [genre for genre in u_item.columns[6:] if row[genre] == 1]
    movie_node = Node("MovieLens", ml_id=row['movie_id'], title=row['movie_title'], genres=genres)
    graph.create(movie_node)


# Create IMDb nodes
# Create IMDb nodes with genres as a list
for index, row in title_basics.iterrows():
    genres = row['genres'].split(',') if pd.notna(row['genres']) else []
    movie_node = Node("IMDb", imdb_id=row['tconst'], title=row['primaryTitle'], genres=genres)
    graph.create(movie_node)



# Create relationships based on mapping file
for index, row in mapping_data.iterrows():
    ml_id = row['ml_id']
    imdb_ids = row['imdb_id'].split('/')
    for imdb_id in imdb_ids:
        match_query = """
        MATCH (ml:MovieLens {ml_id: $ml_id}), (imdb:IMDb {imdb_id: $imdb_id})
        CREATE (ml)-[:RELATED_TO]->(imdb)
        """
        graph.run(match_query, ml_id=ml_id, imdb_id=imdb_id)

print("Data upload and relationships creation completed.")

# Add IMDb ratings
for index, row in title_ratings.iterrows():
    match_query = """
    MATCH (imdb:IMDb {imdb_id: $imdb_id})
    SET imdb.averageRating = $averageRating, imdb.numVotes = $numVotes
    """
    graph.run(match_query, imdb_id=row['tconst'], averageRating=row['averageRating'], numVotes=row['numVotes'])

print("IMDb ratings added.")