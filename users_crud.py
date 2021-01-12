from psycopg2 import connect, DatabaseError
from database_connection import db_connect
import logging
from datetime import datetime
from traceback import print_exc

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
logger = logging.getLogger("users-database")
handler = logging.FileHandler('mylog.log')
# create a logging format
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


def add_user(email, username):
    try:
        conn = db_connect()
        cursor = conn.cursor()

        insert_user = '''INSERT INTO users (
            email,
            username
            )
            VALUES (%s, %s)
        '''

        cursor.execute(insert_user, (email, username))
        conn.commit()
        logger.info(f"{username} successfully inserted to database!")

    except (Exception, DatabaseError) as error:
        print_exc()

    finally:
        if conn is not None:
            conn.close()


def update_user(user_id, new_username=None, new_email=None):
    # Passed arguments to update episode
    passed_args = locals()
    del passed_args["user_id"]

    try:
        conn = db_connect()
        cursor = conn.cursor()
        
        get_current = "SELECT email, username FROM users WHERE user_id = %s"
        cursor.execute(get_current, (user_id,))
        current_email, current_username = cursor.fetchone()
        current_user = [current_username, current_email]

        UPDATE = False
        for i, field in enumerate(passed_args.keys()):
            if passed_args[field]:
               if passed_args[field] != current_user[i]:
                    UPDATE = True
            else:
                passed_args[field] = current_user[i]

        if UPDATE:
            update_user = '''UPDATE users SET
                email = %s,
                username = %s
                WHERE user_id = %s
            '''
            cursor.execute(update_user, (
                passed_args["new_email"], 
                passed_args["new_username"],
                user_id
                )
            )
            logger.info(f"{new_username} was successfully updated!")

        else:
            logger.info("There is no field to update.")

        conn.commit()

    except (Exception, DatabaseError) as error:
        print_exc()

    finally:
        if conn is not None:
            conn.close()


def delete_user(user_id):
    try:
        conn = db_connect()
        cursor = conn.cursor()

        delete_show = "DELETE FROM users WHERE user_id = %s"
        cursor.execute(delete_show, (user_id, ))
        count = cursor.rowcount
        conn.commit()

        if count:
            logger.info(f"User successfully deleted!")
        else:
            logger.info(f"This user does not exist in our database")


    except (Exception, DatabaseError) as error:
        print_exc()

    finally:
        if conn is not None:
            conn.close()


def retrieve_user(user_id=None):
    try:
        conn = db_connect()
        cursor = conn.cursor()

        if user_id:
            retrieve_user = "SELECT user_id, username, email, created_at FROM users WHERE user_id = %s"
            cursor.execute(retrieve_user, (user_id, ))
            results = cursor.fetchone()
        else:
            retrieve_user = "SELECT user_id, email, username, created_at FROM users"
            cursor.execute(retrieve_user, (user_id, ))
            results = cursor.fetchall()

        if results:
            users = [{"user_id": result[0], "username": result[1], "email": result[2], "registered at": datetime.strftime(result[3], "%H:%M:%S - %d/%m/%Y")} for result in results]
            
        else:
            logger.info("There is no user with this id")
            return

        logger.info(f"{users}")
        
        return users

    except (Exception, DatabaseError) as error:
        print_exc()

    finally:
        if conn is not None:
            conn.close()