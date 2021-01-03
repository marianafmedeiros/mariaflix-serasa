from psycopg2 import connect, Error
import json

with open("connections.json","r") as json_file:
    my_credentials = json.load(json_file)["postgresql_local"]

def db_connect(credentials):
    hostname, username, password, database = credentials.values()

    try:
        conn = connect(
            user = username,
            password = password,
            host = hostname,
            database = database
        )
        # Create a cursor to perform database operations
        cursor = conn.cursor()
        # Print PostgreSQL details
        print("PostgreSQL server information")
        print(conn.get_dsn_parameters(), "\n")
        # Executing a SQL query
        cursor.execute("SELECT version();")
        # Fetch result
        record = cursor.fetchone()
        print("You are connected to - ", record, "\n")

        return conn

    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)


conn = db_connect(my_credentials)
if conn:
    conn.close()
    print("PostgreSQL connection is closed")
