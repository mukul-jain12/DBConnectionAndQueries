"""
    @File :   db_connection.py
    @Author : mukul
    @Date :   27-12-2021
"""
import os
from getpass import getpass
from mysql.connector import connect, Error
from dotenv import load_dotenv

load_dotenv()


class DBConnection:

    @staticmethod
    def establish_connection():
        """
            desc: Established database connection and perform query to created database
        """
        try:
            connection = connect(
                    host=os.getenv('host'),
                    user=os.getenv('user'),
                    password=os.getenv('pswd'),
                    database="online_movie_rating",
            )
            return connection
        except Error as e:
            print(e)
