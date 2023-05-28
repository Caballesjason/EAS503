import pandas as pd
from utility_functions import create_connection, create_table, execute_sql_statement

raw_csv = 'netflix_titles.csv'


# Converting Data in First Normal Form
# We want to create tables that have access to keys for any non atomic values


def film_table():
    raw_csv = 'netflix_titles.csv'
    database = 'netflix.db'
    conn = create_connection(database)
    data = pd.read_csv(raw_csv)
    month_added = []
    year_added = []
    show_ID = list(map(lambda x:int(x.strip('s')), list(data.show_id)))
    film_type = list(data.type)
    titles = list(data.title)
    year_released = list(map(int, list(data.release_year)))
    ratings = list(data.rating)
    description = list(data.description)

    for date in list(data.date_added):
        try:
            date_list = date.split(" ")
            month_added.append(date_list[0])
            year_added.append(int(date_list[2]))
        except:
            month_added.append("NaN")
            year_added.append(-1)


    data = list(zip(show_ID, titles, film_type, month_added, year_added, year_released, ratings, description))
  
    
    tbl_query = """
        CREATE TABLE IF NOT EXISTS Films (
        FILM_ID INTEGER NOT NULL PRIMARY KEY,
        TITLE TEXT NOT NULL,
        FILM_TYPE TEXT NOT NULL,
        MONTH_ADDED TEXT,
        YEAR_ADDED INTEGER,
        YEAR_RELEASED INTEGER,
        RATING TEXT,
        DESCRIPTION TEXT
        );
    """
    
    data_insert_query = """
    INSERT INTO Films (FILM_ID, TITLE, FILM_TYPE, MONTH_ADDED, YEAR_ADDED, YEAR_RELEASED, RATING, DESCRIPTION) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    cur = conn.cursor()
    create_table(conn, tbl_query, 'Films')
    with conn:
        cur.executemany(data_insert_query, data)

def actor_table():
    raw_csv = 'netflix_titles.csv'
    database = 'netflix.db'
    conn = create_connection(database)
    actor_column = []
    
    data = pd.read_csv(raw_csv)
    actors = list(data.cast)
    for characters in actors:
        try:
            list_Of_char = characters.split(',')
            actor_column += list_Of_char
        except:
            pass
        
    actor_column = list(map(lambda x: x.lstrip().rstrip(), actor_column))
    actor_column = sorted(list(set(actor_column)))
    actorid = [key for key in range(1, len(actor_column)+1)]
    data =  list(zip(actor_column, actorid))
    
    tbl_query = """
   CREATE TABLE IF NOT EXISTS Actors (
        ACTOR_NAME TEXT,
        ACTOR_ID INTEGER NOT NULL PRIMARY KEY
        );
    """
    
    data_insert_query = """
    INSERT INTO Actors (ACTOR_NAME, ACTOR_ID) VALUES (?, ?)
    """
    
    cur = conn.cursor()
    create_table(conn, tbl_query, 'Actors')
    with conn:
        cur.executemany(data_insert_query, data)
        
def genre_table():
    raw_csv = 'netflix_titles.csv'
    database = 'netflix.db'
    conn = create_connection(database)
    
    genre_column = []
    data = pd.read_csv(raw_csv)
    genres = list(data.listed_in)
    
    for genre_string in genres:
        genre_string_list = genre_string.split(',')
        genre_column += genre_string_list
    
    genre_column = list(map(lambda x: x.lstrip().rstrip(), genre_column))

    genre_column = sorted(list(set(genre_column)))
    genreid = [key for key in range(1, len(genre_column)+1)]
    data =  list(zip(genre_column, genreid))
    
    tbl_query = """
   CREATE TABLE IF NOT EXISTS Genres (
        GENRE TEXT,
        GENRE_ID INTEGER NOT NULL PRIMARY KEY
        );
    """
    
    data_insert_query = """
    INSERT INTO Genres (GENRE, GENRE_ID) VALUES (?, ?)
    """
    
    cur = conn.cursor()
    create_table(conn, tbl_query, 'Genres')
    with conn:
        cur.executemany(data_insert_query, data)

def countries_table():
    raw_csv = 'netflix_titles.csv'
    database = 'netflix.db'
    conn = create_connection(database)
    
    countries_column = []
    data = pd.read_csv(raw_csv)
    countries = list(data.country)
    
    for country_string in countries:
        try:
            country_string_list = country_string.split(',')
            countries_column += country_string_list
        except:
            pass

    countries_column = list(map(lambda x: x.lstrip().rstrip(), countries_column))
    countries_column = list(filter(lambda x: len(x) != 0, countries_column))
    countries_column = sorted(list(set(countries_column)))
    countryid = [key for key in range(1, len(countries_column)+1)]
    data =  list(zip(countries_column, countryid))
    
    tbl_query = """
   CREATE TABLE IF NOT EXISTS Countries (
        COUNTRIES TEXT,
        COUNTRY_ID INTEGER NOT NULL PRIMARY KEY
        );
    """
    
    data_insert_query = """
    INSERT INTO Countries (COUNTRIES, COUNTRY_ID) VALUES (?, ?)
    """
    
    cur = conn.cursor()
    create_table(conn, tbl_query, 'Countries')
    with conn:
        cur.executemany(data_insert_query, data)


def directors_table():

    raw_csv = 'netflix_titles.csv'
    database = 'netflix.db'
    conn = create_connection(database)
    
    directors_column = []
    data = pd.read_csv(raw_csv)
    directors = list(data.director)
    
    for director_string in directors:
        try:
            director_string_list = director_string.split(',')
            directors_column += director_string_list
        except:
            pass

    directors_column = list(map(lambda x: x.lstrip().rstrip(), directors_column))

    directors_column = sorted(list(set(directors_column)))
    directorid = [key for key in range(1, len(directors_column)+1)]
    data =  list(zip(directors_column, directorid))
    
    tbl_query = """
   CREATE TABLE IF NOT EXISTS Directors (
        DIRECTORS TEXT,
        DIRECTOR_ID INTEGER NOT NULL PRIMARY KEY
        );
    """
    
    data_insert_query = """
    INSERT INTO Directors (DIRECTORS, DIRECTOR_ID) VALUES (?, ?)
    """
    
    cur = conn.cursor()
    create_table(conn, tbl_query, 'Directors')
    with conn:
        cur.executemany(data_insert_query, data)

# You require the actor, genre, country, director, id dictionary functions

def genre_id_dict():
    database = 'netflix.db'
    conn = create_connection(database)
    sql = "SELECT * FROM Genres"
    data = pd.read_sql(sql, conn)
    keys = list(data.GENRE)
    vals = list(data.GENRE_ID)
    dict_ = {key: val for key,val in zip(keys, vals)}
    return dict_

def actor_id_dict():
    conn = create_connection("netflix.db")
    sql = "SELECT * FROM Actors"
    data = pd.read_sql(sql, conn)
    keys = list(data.ACTOR_NAME)
    vals = list(data.ACTOR_ID)
    dict_ = {key: val for key,val in zip(keys, vals)}
    return dict_

def country_id_dict():
    conn = create_connection("Netflix.db")
    sql = "SELECT * FROM Countries"
    data = pd.read_sql(sql, conn)
    keys = list(data.COUNTRIES)
    vals = list(data.COUNTRY_ID)
    dict_ = {key: val for key,val in zip(keys, vals)}
    return dict_

def director_id_dict():
    sql = "SELECT * FROM Directors"
    conn = create_connection('netflix.db')
    data = pd.read_sql(sql, conn)
    keys = list(data.DIRECTORS)
    vals = list(data.DIRECTOR_ID)
    dict_ = {key: val for key,val in zip(keys, vals)}
    return dict_

def film_genre_table():
    raw_csv = 'netflix_titles.csv'
    database = 'netflix.db'
    conn = create_connection(database)
    data = pd.read_csv(raw_csv)

    show_ID = list(map(lambda x:int(x.strip('s')), list(data.show_id)))
    data_show_id = list(data.show_id)
    genre_id_dictionary = genre_id_dict()
    film_id_dict = {data_show_id[index]: show_ID[index] for index in range(len(show_ID))}
    values = []

    for index, row in data.iterrows():
        film_id = film_id_dict.get(row['show_id'])
        genres = row['listed_in'].split(',')
        genres = list(map(lambda x: x.lstrip().rstrip(), genres))
        film_id = [film_id]*len(genres)
        genre_ids = [genre_id_dictionary.get(index) for index in genres]
        values += list(zip(film_id, genre_ids, genres))
    

    tbl_query = """
        CREATE TABLE IF NOT EXISTS Films_And_Genres (
                FILM_ID INTEGER NOT NULL,
                GENRE_ID INTEGER,
                GENRE TEXT,
                FOREIGN KEY(FILM_ID) REFERENCES Films(FILM_ID),
                FOREIGN KEY(GENRE_ID) REFERENCES Genres(GENRE_ID)
                );
            """
    
    data_insert_query = """
        INSERT INTO Films_And_Genres (FILM_ID, GENRE_ID, GENRE) VALUES (?, ?, ?)
        """
    
    cur = conn.cursor()
    create_table(conn, tbl_query, 'Films_And_Genres')
    with conn:
        cur.executemany(data_insert_query, values)

def film_director_table():
    raw_csv = 'netflix_titles.csv'
    database = 'netflix.db'
    conn = create_connection(database)
    data = pd.read_csv(raw_csv)

    show_ID = list(map(lambda x:int(x.strip('s')), list(data.show_id)))
    data_show_id = list(data.show_id)
    director_id_dictionary = director_id_dict()
    film_id_dict = {data_show_id[index]: show_ID[index] for index in range(len(show_ID))}

    values = []

    def help_null(val, dict_):
        try:
            return dict_.get(val)
        except:
            return (None, )

    for index, row in data.iterrows():
        film_id = film_id_dict.get(row['show_id'])
        try:
            directors = row['director'].split(',')
            directors = list(map(lambda x: x.lstrip().rstrip(), directors))
        except:
            directors = [None]
        film_id = [film_id]*len(directors)
        director_ids = [help_null(index, director_id_dictionary) for index in directors]
        values += list(zip(film_id, director_ids, directors))
    

    tbl_query = """
        CREATE TABLE Films_And_Directors (
                FILM_ID INTEGER NOT NULL,
                DIRECTOR_ID INTEGER,
                DIRECTOR TEXT,
                FOREIGN KEY(FILM_ID) REFERENCES Films(FILM_ID),
                FOREIGN KEY(DIRECTOR_ID) REFERENCES Directors(DIRECTOR_ID)
                );
            """
    
    data_insert_query = """
        INSERT INTO Films_And_Directors (FILM_ID, DIRECTOR_ID, DIRECTOR) VALUES (?, ?, ?)
        """
    
    cur = conn.cursor()
    create_table(conn, tbl_query, 'Films_And_Directors')
    with conn:
        cur.executemany(data_insert_query, values)

def film_actor_table():
    raw_csv = 'netflix_titles.csv'
    database = 'netflix.db'
    conn = create_connection(database)
    data = pd.read_csv(raw_csv)

    show_ID = list(map(lambda x:int(x.strip('s')), list(data.show_id)))
    data_show_id = list(data.show_id)
    actor_id_dictionary = actor_id_dict()
    film_id_dict = {data_show_id[index]: show_ID[index] for index in range(len(show_ID))}

    values = []

# help_null returns a tuple with a none type for missing data
    def help_null(val, dict_):
        try:
            return dict_.get(val)
        except:
            return (None, )

    for index, row in data.iterrows():
        film_id = film_id_dict.get(row['show_id'])
        try:
            casts = row['cast'].split(',')
            casts = list(map(lambda x: x.lstrip().rstrip(), casts))
        except:
            casts = [None]
        film_id = [film_id]*len(casts)
        cast_ids = [help_null(index, actor_id_dictionary) for index in casts]
        values += list(zip(film_id, cast_ids, casts))
    

    tbl_query = """
        CREATE TABLE Films_And_Actors (
                FILM_ID INTEGER NOT NULL,
                ACTOR_ID INTEGER,
                ACTOR TEXT,
                FOREIGN KEY(FILM_ID) REFERENCES Films(FILM_ID),
                FOREIGN KEY(ACTOR_ID) REFERENCES Directors(ACTOR_ID)
                );
            """
    
    data_insert_query = """
        INSERT INTO Films_And_Actors (FILM_ID, ACTOR_ID, ACTOR) VALUES (?, ?, ?)
        """
    
    cur = conn.cursor()
    create_table(conn, tbl_query, 'Films_And_Actors')
    with conn:
        cur.executemany(data_insert_query, values)

def film_country_table():
    raw_csv = 'netflix_titles.csv'
    database = 'netflix.db'
    conn = create_connection(database)
    data = pd.read_csv(raw_csv)

    show_ID = list(map(lambda x:int(x.strip('s')), list(data.show_id)))
    data_show_id = list(data.show_id)
    country_id_dictionary = country_id_dict()
    film_id_dict = {data_show_id[index]: show_ID[index] for index in range(len(show_ID))}

    values = []
    
    def help_null(val, dict_):
        try:
            return dict_.get(val)
        except:
            return (None, )
    for index, row in data.iterrows():
        film_id = film_id_dict.get(row['show_id'])
        try:
            countries = row['country'].split(',')
            countries = list(map(lambda x: x.lstrip().rstrip(), countries))
        except:
            countries = [None]
        film_id = [film_id]*len(countries)
        country_ids = [help_null(index, country_id_dictionary) for index in countries]
        values += list(zip(film_id, country_ids, countries))
    

    tbl_query = """
        CREATE TABLE Films_And_Countries (
                FILM_ID INTEGER NOT NULL,
                COUNTRY_ID INTEGER,
                COUNTRIES TEXT,
                FOREIGN KEY(FILM_ID) REFERENCES Films(FILM_ID),
                FOREIGN KEY(COUNTRY_ID) REFERENCES Directors(COUNTRY_ID)
                );
            """
    
    data_insert_query = """
        INSERT INTO Films_And_Countries (FILM_ID, COUNTRY_ID, COUNTRIES) VALUES (?, ?, ?)
        """
    
    cur = conn.cursor()
    create_table(conn, tbl_query, 'Films_And_Countries')
    with conn:
        cur.executemany(data_insert_query, values)

def month_table():
    database = 'netflix.db'
    conn = create_connection(database)
    months = [
        'January',
        'February',
        'March',
        'April',
        'May',
        'June',
        'July',
        'August',
        'September',
        'October',
        'November',
        'December'
    ]
    ids = [index for index in range(1, len(months)+1)]

    values = list(zip(months, ids))

    table_sql = """
    CREATE TABLE Months (
        MONTHS TEXT,
        MONTHID INTEGER NOT NULL
        )
        """

    values_sql = """INSERT INTO Months VALUES (?, ?)"""

    create_table(conn, table_sql, "Months")
    with conn:
        cur = conn.cursor()
        cur.executemany(values_sql, values)

month_table()
film_table()
actor_table()
countries_table()
directors_table()
genre_table()
film_genre_table()
film_director_table()
film_country_table()
film_actor_table()
