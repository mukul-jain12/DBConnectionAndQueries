"""
    @File :   db_queries.py
    @Author : mukul
    @Date :   26-12-2021
"""
from db_connection import DBConnection


class DBQuery:

    def __init__(self):
        self.connection = DBConnection.establish_connection()
        self.cursor = self.connection.cursor()

    def create_database(self):
        """
            desc: creating databases
        """
        create_db_query = "CREATE DATABASE online_movie_rating"
        self.cursor.execute(create_db_query)
        self.connection.commit()

    def create_movie_table(self):
        """
            desc: creating movie table
            parameter: id, title, release year, genre, collection_in_mil
        """
        movies_table_query = """
        CREATE TABLE movies(
            id INT AUTO_INCREMENT PRIMARY KEY,
            title VARCHAR(100),
            release_year YEAR(4),
            genre VARCHAR(100),
            collection_in_mil INT
        )
        """
        self.cursor.execute(movies_table_query)
        self.connection.commit()

    def create_reviewer_table(self):
        """
            desc: creating reviewer table
            parameter: id, first name, last name
        """
        reviewers_table_query = """
        CREATE TABLE reviewers (
            id INT AUTO_INCREMENT PRIMARY KEY,
            first_name VARCHAR(100),
            last_name VARCHAR(100)
        )
        """
        self.cursor.execute(reviewers_table_query)
        self.connection.commit()

    def create_rating_table(self):
        """
            desc: creating rating table
            parameter: movie_id, reviewer_id rating
        """
        ratings_table_query = """
        CREATE TABLE ratings (
            movie_id INT,
            reviewer_id INT,
            rating DECIMAL(2,1),
            FOREIGN KEY(movie_id) REFERENCES movies(id),
            FOREIGN KEY(reviewer_id) REFERENCES reviewers(id),
            PRIMARY KEY(movie_id, reviewer_id)
        )
        """
        self.cursor.execute(ratings_table_query)
        self.connection.commit()

    def alter_table(self):
        """
            desc: alter the movie table
        """
        alter_table_query = """
            ALTER TABLE movies MODIFY COLUMN collection_in_mil DECIMAL(4,1)
            """
        self.cursor.execute(alter_table_query)
        self.connection.commit()

    def insert_movie_data(self):
        """
            desc: inserting the data in the movie table
        """
        insert_movies_query = """
        INSERT INTO movies (title, release_year, genre, collection_in_mil)
        VALUES
            ("Forrest Gump", 1994, "Drama", 330.2),
            ("3 Idiots", 2009, "Drama", 2.4),
            ("Eternal Sunshine of the Spotless Mind", 2004, "Drama", 34.5),
            ("Good Will Hunting", 1997, "Drama", 138.1),
            ("Skyfall", 2012, "Action", 304.6),
            ("Gladiator", 2000, "Action", 188.7),
            ("Black", 2005, "Drama", 3.0),
            ("Titanic", 1997, "Romance", 659.2),
            ("The Shawshank Redemption", 1994, "Drama",28.4),
            ("Udaan", 2010, "Drama", 1.5),
            ("Home Alone", 1990, "Comedy", 286.9),
            ("Casablanca", 1942, "Romance", 1.0),
            ("Avengers: Endgame", 2019, "Action", 858.8),
            ("Night of the Living Dead", 1968, "Horror", 2.5),
            ("The Godfather", 1972, "Crime", 135.6),
            ("Haider", 2014, "Action", 4.2),
            ("Inception", 2010, "Adventure", 293.7),
            ("Evil", 2003, "Horror", 1.3),
            ("Toy Story 4", 2019, "Animation", 434.9),
            ("Air Force One", 1997, "Drama", 138.1),
            ("The Dark Knight", 2008, "Action",535.4),
            ("Bhaag Milkha Bhaag", 2013, "Sport", 4.1),
            ("The Lion King", 1994, "Animation", 423.6),
            ("Pulp Fiction", 1994, "Crime", 108.8),
            ("Kai Po Che", 2013, "Sport", 6.0),
            ("Beasts of No Nation", 2015, "War", 1.4),
            ("Andadhun", 2018, "Thriller", 2.9),
            ("The Silence of the Lambs", 1991, "Crime", 68.2),
            ("Deadpool", 2016, "Action", 363.6),
            ("Drishyam", 2015, "Mystery", 3.0)
        """
        self.cursor.execute(insert_movies_query)
        self.connection.commit()

    def insert_reviewer_data(self):
        """
            desc: inserting the data in the reviewer table
        """
        insert_reviewers_query = """
        INSERT INTO reviewers
        (first_name, last_name)
        VALUES ("Chaitanya", "Baweja"),
            ("Mary", "Cooper"),
            ("John", "Wayne"),
            ("Thomas", "Stoneman"),
            ("Penny", "Hofstadter"),
            ("Mitchell", "Marsh"),
            ("Wyatt", "Skaggs"),
            ("Andre", "Veiga"),
            ("Sheldon", "Cooper"),
            ("Kimbra", "Masters"),
            ("Kat", "Dennings"),
            ("Bruce", "Wayne"),
            ("Domingo", "Cortes"),
            ("Rajesh", "Koothrappali"),
            ("Ben", "Glocker"),
            ("Mahinder", "Dhoni"),
            ("Akbar", "Khan"),
            ("Howard", "Wolowitz"),
            ("Pinkie", "Petit"),
            ("Gurkaran", "Singh"),
            ("Amy", "Farah Fowler"),
            ("Marlon", "Crafford")
        """
        self.cursor.execute(insert_reviewers_query)
        self.connection.commit()

    def insert_ratings_data(self):
        """
            desc: inserting the data in the ratings table
        """
        insert_ratings_query = """
        INSERT INTO ratings
        (rating, movie_id, reviewer_id)
        VALUES (6.4, 17, 5), (5.6, 19, 1), (6.3, 22, 14), (5.1, 21, 17),
            (5.0, 5, 5), (6.5, 21, 5), (8.5, 30, 13), (9.7, 6, 4),
            (8.5, 24, 12), (9.9, 14, 9), (8.7, 26, 14), (9.9, 6, 10),
            (5.1, 30, 6), (5.4, 18, 16), (6.2, 6, 20), (7.3, 21, 19),
            (8.1, 17, 18), (5.0, 7, 2), (9.8, 23, 3), (8.0, 22, 9),
            (8.5, 11, 13), (5.0, 5, 11), (5.7, 8, 2), (7.6, 25, 19),
            (5.2, 18, 15), (9.7, 13, 3), (5.8, 18, 8), (5.8, 30, 15),
            (8.4, 21, 18), (6.2, 23, 16), (7.0, 10, 18), (9.5, 30, 20),
            (8.9, 3, 19), (6.4, 12, 2), (7.8, 12, 22), (9.9, 15, 13),
            (7.5, 20, 17), (9.0, 25, 6), (8.5, 23, 2), (5.3, 30, 17),
            (6.4, 5, 10), (8.1, 5, 21), (5.7, 22, 1), (6.3, 28, 4),
            (9.8, 13, 1)
        """
        self.cursor.execute(insert_ratings_query)
        self.connection.commit()

    def delete_table(self):
        """
            desc: delete the table
        """
        delete_query = "drop table movies"
        self.cursor.execute(delete_query)
        self.connection.commit()

    def show_tables(self):
        """
            desc: show the data from the table
        """
        show_data_query = "select * from movies"
        self.cursor.execute(show_data_query)
        for database in self.cursor:
            print(database)

    def handle_data_using_join(self):
        """
            desc: show the data from the table
        """
        join_query = "select title, AVG(rating) as avg_rating from ratings " \
                     "inner join movies ON movies.id = ratings.movie_id GROUP BY movie_id " \
                     "ORDER BY avg_rating DESC " \
                     "limit 5;"
        self.cursor.execute(join_query)
        for movie in self.cursor.fetchall():
            print(movie)

    def update_table(self):
        """
            desc: show the data from the table
        """
        update_query = """update reviewers set last_name = "Cooper" where first_name = "Amy" """
        self.cursor.execute(update_query)
        for movie in self.cursor.fetchall():
            print(movie)


if __name__ == "__main__":
    db_query = DBQuery()
    db_query.create_database()
    db_query.create_movie_table()
    db_query.create_reviewer_table()
    db_query.create_rating_table()
    db_query.insert_movie_data()
    db_query.insert_reviewer_data()
    db_query.insert_ratings_data()
    db_query.alter_table()
    db_query.delete_table()
    db_query.show_tables()
    db_query.handle_data_using_join()
    db_query.update_table()
