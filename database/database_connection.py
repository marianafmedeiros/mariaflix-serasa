from psycopg2 import connect, Error
import json

with open("./database/credentials_public.json","r") as json_file:
    my_credentials = json.load(json_file)["postgresql_heroku"]

def db_connect():
    hostname, username, password, database = my_credentials.values()

    try:
        conn = connect(
            user = username,
            password = password,
            host = hostname,
            database = database
        )

        return conn

    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)

