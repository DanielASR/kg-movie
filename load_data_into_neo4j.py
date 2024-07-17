# Load datasets
import pandas as pd
from py2neo import Graph, Node, Relationship
from neo4j import GraphDatabase

# Load MovieLens data
u_data = pd.read_csv('movielens/u.data', sep='\t', names=['user_id', 'item_id', 'rating', 'timestamp'], dtype={'user_id': int, 'item_id': int, 'rating': int, 'timestamp': int})
u_item = pd.read_csv('movielens/u.item', sep='|', encoding='ISO-8859-1', names=['movie_id', 'movie_title', 'release_date', 'video_release_date', 'IMDb_URL', 'unknown', 'Action', 'Adventure', 'Animation', 'Children', 'Comedy', 'Crime', 'Documentary', 'Drama', 'Fantasy', 'Film-Noir', 'Horror', 'Musical', 'Mystery', 'Romance', 'Sci-Fi', 'Thriller', 'War', 'Western'], dtype={'movie_id': int})
u_user = pd.read_csv('movielens/u.user', sep='|', names=['user_id', 'age', 'gender', 'occupation', 'zip_code'], dtype={'user_id': int, 'age': int})


# Load IMDB data
title_basics = pd.read_csv('IMDB/title.basics.tsv', sep='\t', na_values='\\N', dtype={'tconst': str, 'primaryTitle': str, 'genres': str}, low_memory=False)
title_ratings = pd.read_csv('IMDB/title.ratings.tsv', sep='\t', na_values='\\N', dtype={'tconst': str})
title_principals = pd.read_csv('IMDB/title.principals.tsv', sep='\t', na_values='\\N', dtype={'tconst': str, 'nconst': str, 'category': str, 'job': str, 'characters': str}, low_memory=False)
name_basics = pd.read_csv('IMDB/name.basics.tsv', sep='\t', na_values='\\N', dtype={'nconst': str, 'primaryName': str}, low_memory=False)

# Load mapping file
mapping_data = pd.read_csv('combined_dataset/matched_title.pd', sep='\t', names=['ml_id', 'imdb_id'], dtype={'ml_id': str})
mapping_data['ml_id'] = pd.to_numeric(mapping_data['ml_id'], errors='coerce').astype('Int64')


# Connect to Neo4j
# Adjust connection details accordingly

# Clear existing data (optional)
# graph.run("MATCH (n) DETACH DELETE n")

# verification
# URI =
# AUTH =
with GraphDatabase.driver(URI, auth=AUTH) as driver:
    driver.verify_connectivity()
    print("Connection established.")

# Create genre nodes
genres = set()
for genre in u_item.columns[6:]:
    genres.add(genre)

for index, row in title_basics.iterrows():
    if pd.notna(row['genres']):
        genres.update(row['genres'].split(','))

for genre in genres:
    genre_node = Node("Genre", name=genre)
    graph.merge(genre_node, "Genre", "name")



# Create User nodes
for index, row in u_user.iterrows():
    user_node = Node("User", user_id=int(row['user_id']), age=int(row['age']), gender=row['gender'], occupation=row['occupation'], zip_code=row['zip_code'])
    graph.merge(user_node, "User", "user_id")




# Create MovieLens nodes with genre relationships and user ratings
for index, row in u_item.iterrows():
    genres = [genre for genre in u_item.columns[6:] if row[genre] == 1]
    movie_node = Node("MovieLens", ml_id=int(row['movie_id']), title=row['movie_title'])
    graph.merge(movie_node, "MovieLens", "ml_id")
    for genre in genres:
        genre_node = graph.nodes.match("Genre", name=genre).first()
        graph.merge(Relationship(movie_node, "HAS_GENRE", genre_node))

    ratings = u_data[u_data['item_id'] == int(row['movie_id'])]
    for _, rating_row in ratings.iterrows():
        user_node = graph.nodes.match("User", user_id=int(rating_row['user_id'])).first()
        graph.merge(Relationship(user_node, "RATED", movie_node, rating=int(rating_row['rating']), timestamp=int(rating_row['timestamp'])))



# Create IMDb nodes with genre relationships
for index, row in title_basics.iterrows():
    movie_node = Node("IMDb", imdb_id=row['tconst'], title=row['primaryTitle'])
    graph.merge(movie_node, "IMDb", "imdb_id")
    if pd.notna(row['genres']):
        genres = row['genres'].split(',')
        for genre in genres:
            genre_node = graph.nodes.match("Genre", name=genre).first()
            graph.merge(Relationship(movie_node, "HAS_GENRE", genre_node))




# Create relationships based on mapping file
for index, row in mapping_data.dropna(subset=['ml_id']).iterrows():
    ml_id = int(row['ml_id'])
    imdb_ids = row['imdb_id'].split('/')
    for imdb_id in imdb_ids:
        match_query = """
        MATCH (ml:MovieLens {ml_id: $ml_id}), (imdb:IMDb {imdb_id: $imdb_id})
        MERGE (ml)-[:RELATED_TO]->(imdb)
        """
        graph.run(match_query, ml_id=ml_id, imdb_id=imdb_id)



print("Data upload and relationships creation completed.")

# Add IMDb ratings
for index, row in title_ratings.iterrows():
    match_query = """
    MATCH (imdb:IMDb {imdb_id: $imdb_id})
    SET imdb.averageRating = $averageRating, imdb.numVotes = $numVotes
    """
    graph.run(match_query, imdb_id=row['tconst'], averageRating=float(row['averageRating']), numVotes=int(row['numVotes']))

# Create actors and directors relationships from IMDb principals
for index, row in title_principals.iterrows():
    movie_node = graph.nodes.match("IMDb", imdb_id=row['tconst']).first()
    person_node = graph.nodes.match("Person", nconst=row['nconst']).first()
    if not person_node:
        person_name = name_basics[name_basics['nconst'] == row['nconst']]['primaryName'].values[0]
        person_node = Node("Person", nconst=row['nconst'], name=person_name)
        graph.merge(person_node, "Person", "nconst")
    if row['category'] == 'actor' or row['category'] == 'actress':
        graph.merge(Relationship(person_node, "ACTED_IN", movie_node, characters=row['characters']))
    elif row['category'] == 'director':
        graph.merge(Relationship(person_node, "DIRECTED", movie_node))


print("Data upload and relationships creation completed.")