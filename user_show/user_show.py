from psycopg2 import connect, DatabaseError
from database.database_connection import db_connect
import logging
from datetime import datetime
from traceback import print_exc

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
logger = logging.getLogger("user-show")
handler = logging.FileHandler('mylog.log')
# create a logging format
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

def start_watching(user_id, show_id):
    try:
        conn = db_connect()
        cursor = conn.cursor()

        is_watching = False
        user_show = "SELECT * FROM user_show WHERE user_id = %s AND show_id = %s"
        cursor.execute(user_show, (user_id, show_id))
        is_watching = bool(cursor.fetchone())

        if not is_watching:
            relate_user_show = '''INSERT INTO user_show (
                user_id,
                show_id
                )
                VALUES (%s, %s)
            '''

            cursor.execute(relate_user_show, (user_id, show_id))
            logger.info(f"User {user_id} is now watching show {show_id}!")

        else:
            logger.info(f"User {user_id} is already watching show {show_id}!")

        conn.commit()

    except (Exception, DatabaseError) as error:
        print_exc()

    finally:
        if conn is not None:
            conn.close()

def finish_watching(user_id, show_id):
    try:
        conn = db_connect()
        cursor = conn.cursor()

        relate_user_show = "UPDATE user_show SET finished_at = %s WHERE user_id = %s AND show_id = %s"

        cursor.execute(relate_user_show, (datetime.now(),user_id, show_id))
        conn.commit()
        logger.info(f"User {user_id} finished watching show {show_id}!")

    except (Exception, DatabaseError) as error:
        print_exc()

    finally:
        if conn is not None:
            conn.close()