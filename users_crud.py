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


def retrieve_user(user_id):
    try:
        conn = db_connect()
        cursor = conn.cursor()

        retrieve_user = "SELECT email, username, created_at FROM users WHERE user_id = %s"
        cursor.execute(retrieve_user, (user_id, ))
        result = cursor.fetchone()
        if result:
            email, username, created_at = result
        else:
            logger.info("There is no user with this id")
            return

        created_at = datetime.strftime(created_at, "%d/%m/%Y")
        user_details = {"email": email, "username": username, "created_at": created_at}
        logger.info(f"{user_details}")
        
        return user_details

    except (Exception, DatabaseError) as error:
        print_exc()

    finally:
        if conn is not None:
            conn.close()