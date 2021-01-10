from psycopg2 import connect, DatabaseError
from database_connection import db_connect
import logging
from datetime import datetime
from traceback import print_exc

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
logger = logging.getLogger("movies-database")
handler = logging.FileHandler('mylog.log')
# create a logging format
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


def add_movie(movie_title, year=None, movie_length=None):
    try:
        conn = db_connect()
        cursor = conn.cursor()

        insert_show = ''' INSERT INTO shows (
            show_type,
            title
            )
            VALUES (%s, %s)
            RETURNING show_id 
        '''

        insert_movie = '''INSERT INTO movies (
            show_id,
            year,
            movie_length
            )
            VALUES (%s, %s, %s)
        '''

        cursor.execute(insert_show, (1, movie_title))
        show_id = cursor.fetchone()[0]
        cursor.execute(insert_movie, (show_id, year, movie_length))
        conn.commit()
        logger.info(f"{movie_title} successfully inserted to database!")

    except (Exception, DatabaseError) as error:
        print_exc()

    finally:
        if conn is not None:
            conn.close()


def update_movie(movie_title, new_movie_title=None, year=None, movie_length=None):
    try:
        conn = db_connect()
        cursor = conn.cursor()
        
        if new_movie_title:
            update_show = ''' UPDATE shows
                SET title = %s
                WHERE title = %s
            '''
            movie_title = new_movie_title
            cursor.execute(update_show, (new_movie_title, movie_title))


        if year or movie_length:
            cursor.execute("SELECT show_id FROM shows WHERE title = %s", (movie_title,))
            # print(cursor.fetchone()[0])
            show_id = cursor.fetchone()[0]
            
            if year and not movie_length:
                update_movie = '''UPDATE movies
                    SET year = %s
                    WHERE show_id = %s
                '''
                cursor.execute(update_movie, (year, show_id))

            elif movie_length and not year:
                update_movie = '''UPDATE movies
                    SET movie_length = %s
                    WHERE show_id = %s
                '''
                cursor.execute(update_movie, (movie_length, show_id))

            else:
                update_movie = '''UPDATE movies
                    SET movie_length = %s, year = %s
                    WHERE show_id = %s
                '''
                cursor.execute(update_movie, (movie_length, year, show_id))

        conn.commit()
        logger.info(f"{movie_title} successfully updated!")

    except (Exception, DatabaseError) as error:
        print_exc()

    finally:
        if conn is not None:
            conn.close()


def delete_movie(movie_title):
    try:
        conn = db_connect()
        cursor = conn.cursor()

        delete_show = "DELETE FROM shows WHERE title = %s"
        cursor.execute(delete_show, (movie_title, ))

        conn.commit()
        logger.info(f"{movie_title} successfully deleted!")

    except (Exception, DatabaseError) as error:
        print_exc()

    finally:
        if conn is not None:
            conn.close()


def retrieve_movie(movie_title):
    try:
        conn = db_connect()
        cursor = conn.cursor()

        retrieve_show = "SELECT show_id FROM shows WHERE title = %s"
        cursor.execute(retrieve_show, (movie_title, ))
        show_id = cursor.fetchone()[0]
        
        retrieve_movie = "SELECT * FROM movies WHERE show_id = %s"
        cursor.execute(retrieve_movie, (show_id, ))
        movie_id, show_id, movie_length, movie_year, created_at = cursor.fetchone()
        conn.commit()

        created_at = datetime.strftime(created_at, "%d/%m/%Y")
        movie_details = {"Title": movie_title, "year": movie_year, "length (minutes)": movie_length, "id": movie_id, "created_at": created_at}
        logger.info(f"{movie_details}")
        
        return movie_details

    except (Exception, DatabaseError) as error:
        print_exc()

    finally:
        if conn is not None:
            conn.close()