from psycopg2 import connect, DatabaseError
from database_connection import db_connect
import logging
from datetime import datetime
from traceback import print_exc

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
logger = logging.getLogger("series-database")
# create a logging format for mylog.log file
handler = logging.FileHandler('mylog.log')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


def add_episode(series_title, episode_number, season, episode_title=None, episode_length=None, release_date=None):
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

        insert_episode = '''INSERT INTO episodes (
            episode_number,
            series_id,
            season,
            show_id,
            episode_length,
            release_date
            )
            VALUES (%s, %s, %s, %s, %s, %s)
        '''

        episode_title = episode_title if episode_title else episode_number


        cursor.execute("SELECT series_id FROM series WHERE series_title=%s", (series_title, ))
        series_id = cursor.fetchone()[0]

        if series_id:
            cursor.execute(insert_show, (2, episode_title))
            show_id = cursor.fetchone()[0]

            cursor.execute(insert_episode, (episode_number, series_id, season, show_id, episode_length, release_date))

        conn.commit()
        logger.info(f"{episode_title} episode successfully inserted to database!")

    except (Exception, DatabaseError) as error:
        print_exc()

    finally:
        if conn is not None:
            conn.close()


def add_series(series_title, release_year=None):
    try:
        conn = db_connect()
        cursor = conn.cursor()

        insert_series = '''INSERT INTO series (
            series_title,
            release_year
            )
            VALUES (%s, %s)
        '''

        cursor.execute(insert_series, (series_title, release_year))
        conn.commit()
        logger.info(f"{series_title} successfully inserted to database!")

    except (Exception, DatabaseError) as error:
        print_exc()

    finally:
        if conn is not None:
            conn.close()


def update_episode(series_title, season, episode_number, series_release_year, new_season=None, new_episode_number=None, new_episode_length=None, new_release_date=None, new_episode_title=None):
    # Passed arguments to update episode
    passed_args = locals()
    del passed_args["series_title"]
    del passed_args["season"]
    del passed_args["episode_number"]
    del passed_args["series_release_year"]
    
    try:
        conn = db_connect()
        cursor = conn.cursor()

        # Retrieve series_id
        retrieve_series = "SELECT series_id FROM series WHERE series_title = %s AND release_year = %s"
        cursor.execute(retrieve_series, (series_title,series_release_year))
        series_id = cursor.fetchone()[0]

        # Retrieve corresponding episode
        retrieve_episode = '''SELECT season, episode_number, episode_length, release_date, show_id FROM episodes 
            WHERE series_id = %s AND season = %s AND episode_number = %s'''
        cursor.execute(retrieve_episode, (series_id, season, episode_number))
        current_episode = list(cursor.fetchone())
        show_id = current_episode.pop()

        # Retrieve episode title
        retrieve_episode_title = "SELECT title FROM shows WHERE show_id = %s"
        cursor.execute(retrieve_episode_title, (show_id,))
        current_title = cursor.fetchone()[0]
        # Current episode fields
        current_episode.append(current_title)

        # Check if any field changed
        UPDATE_EPISODE = False
        UPDATE_TITLE = False

        for i, field in enumerate(passed_args.keys()):
            if passed_args[field]:
                if field == "new_episode_title":
                    if passed_args[field] != current_episode[i]:
                        UPDATE_TITLE = True
                        
                elif passed_args[field] != current_episode[i]:
                    UPDATE_EPISODE = True
            else:
                passed_args[field] = current_episode[i]

        if UPDATE_EPISODE or UPDATE_TITLE:
            if UPDATE_TITLE:
                update_title_query = '''UPDATE shows SET title = %s WHERE title = %s'''
                cursor.execute(update_title_query, (passed_args["new_episode_title"], current_title))

            elif UPDATE_EPISODE:
                update_episode_query = '''UPDATE episodes SET
                    season = %s, 
                    episode_number = %s, 
                    episode_length = %s, 
                    release_date = %s
                    WHERE series_id = %s AND season = %s AND episode_number = %s
                '''
                cursor.execute(update_episode_query, (
                    passed_args["new_season"],
                    passed_args["new_episode_number"], 
                    passed_args["new_episode_length"],
                    passed_args["new_release_date"],
                    series_id,
                    season,
                    episode_number)
                )

            logger.info(f"{current_title} episode successfully updated!")

        else:
            logger.info("There is no field to update.")

        conn.commit()

    except (Exception, DatabaseError) as error:
        print_exc()

    finally:
        if conn is not None:
            conn.close()


def update_series(series_title, release_year, new_series_title=None, new_release_year=None):
    # Passed arguments to update episode
    passed_args = locals()
    del passed_args["series_title"]
    del passed_args["release_year"]
    
    try:
        conn = db_connect()
        cursor = conn.cursor()

        current_series = [series_title, release_year]
        
        # Check if it exists
        retrieve_series = "SELECT series_id FROM series WHERE series_title = %s AND release_year = %s"
        cursor.execute(retrieve_series, (series_title, release_year))
        EXISTS = cursor.fetchone() is not None
        
        if not EXISTS:
            logger.info("There is no series with this name and release year")
            return
        
        # Check if any field changed
        UPDATE = False

        for i, field in enumerate(passed_args.keys()):
            if passed_args[field]:
               if passed_args[field] != current_series[i]:
                    UPDATE = True
            else:
                passed_args[field] = current_series[i]

        if UPDATE:
            update_series = '''UPDATE series SET
                series_title = %s,
                release_year = %s
                WHERE series_title = %s AND release_year = %s
            '''
            cursor.execute(update_series, (
                passed_args["new_series_title"], 
                passed_args["new_release_year"],
                series_title,
                release_year
                )
            )
            logger.info(f"{series_title} series successfully updated!")

        else:
            logger.info("There is no field to update.")

        conn.commit()

    except (Exception, DatabaseError) as error:
        print_exc()

    finally:
        if conn is not None:
            conn.close()


def delete_episode(series_title, series_release_year, season, episode_number):
    try:
        conn = db_connect()
        cursor = conn.cursor()

        # Retrieve series_id
        retrieve_series = "SELECT series_id FROM series WHERE series_title = %s AND release_year = %s"
        cursor.execute(retrieve_series, (series_title, series_release_year))
        series_id = cursor.fetchone()[0]

        # Retrieve show_id
        retrieve_show_id = "SELECT show_id FROM episodes WHERE episode_number = %s AND series_id = %s AND season = %s"
        cursor.execute(retrieve_show_id, (episode_number, series_id, season ))
        show_id = cursor.fetchone()
        if show_id:
            show_id = show_id[0]
        else:
            logger.info(f"Episode {episode_number} from season {season} of {series_title} does not exist in our database")
            return

        # Delete from shows 
        delete_show = "DELETE FROM shows WHERE show_id = %s"
        cursor.execute(delete_show, (show_id, ))

        conn.commit()

        logger.info(f"{episode_number} episode from {series_title} successfully deleted!")


    except (Exception, DatabaseError) as error:
        print_exc()

    finally:
        if conn is not None:
            conn.close()


def delete_series(series_title, series_release_year):
    try:
        conn = db_connect()
        cursor = conn.cursor()

        # Retrieve series_id
        retrieve_series = "SELECT series_id FROM series WHERE series_title = %s AND release_year = %s"
        cursor.execute(retrieve_series, (series_title, series_release_year))
        series_id = cursor.fetchone()
        if series_id:
            series_id = series_id[0]
        else:
            logger.info(f"{series_title} does not exist in our database")
            return

        # Retrieve all show_id
        retrieve_show_id = "SELECT show_id FROM episodes WHERE series_id = %s"
        cursor.execute(retrieve_show_id, (series_id, ))
        show_ids = cursor.fetchall()

        # Delete from series
        delete_series = "DELETE FROM series WHERE series_title = %s AND release_year = %s"
        cursor.execute(delete_series, (series_title, series_release_year))

        # Delete respective episodes from shows
        if show_ids:
            show_id_list = [show_id[0] for show_id in show_ids]
            show_id_tuple = tuple(show_id_list)
            delete_episodes = "DELETE FROM shows WHERE show_id IN %s"
            cursor.execute(delete_episodes, (show_id_tuple,))

        conn.commit()
        logger.info(f"{series_title} successfully deleted!")

    except (Exception, DatabaseError) as error:
        print_exc()

    finally:
        if conn is not None:
            conn.close()


def retrieve_episode(series_title, series_release_year, season, episode_number):
    try:
        conn = db_connect()
        cursor = conn.cursor()

        # Retrieve series_id
        retrieve_series = "SELECT series_id FROM series WHERE series_title = %s AND release_year = %s"
        cursor.execute(retrieve_series, (series_title, series_release_year))
        series_id = cursor.fetchone()[0]

        retrieve_episode = "SELECT episode_length, release_date, created_at, show_id FROM episodes WHERE series_id = %s AND season = %s AND episode_number = %s"
        cursor.execute(retrieve_episode, (series_id, season, episode_number))
        episode_length, release_date, created_at, show_id = cursor.fetchone()
        release_date = datetime.strftime(release_date, "%d/%m/%Y")
        created_at = datetime.strftime(created_at, "%d/%m/%Y")

        retrieve_episode_title = "SELECT title FROM shows WHERE show_id = %s"
        cursor.execute(retrieve_episode_title, (show_id,))
        title = cursor.fetchone()[0]
        
        episode_details = {
            "title": title, 
            "episode_number": episode_number, 
            "series_id": series_id,
            "season": season,
            "show_id": show_id, 
            "length (minutes)": episode_length, 
            "release_date": release_date, 
            "created_at": created_at
            }

        logger.info(f"{episode_details}")

        return episode_details

    except (Exception, DatabaseError) as error:
        print_exc()

    finally:
        if conn is not None:
            conn.close()


def retrieve_series(series_id):
    try:
        conn = db_connect()
        cursor = conn.cursor()

        retrieve_series = "SELECT * FROM series WHERE series_id = %s"
        cursor.execute(retrieve_series, (series_id, ))
        series_id, title, release_year, created_at = cursor.fetchone()
        created_at = datetime.strftime(created_at, "%H:%M:%S - %d/%m/%Y")

        # Retrieve how many seasons
        get_seasons = "SELECT COUNT(DISTINCT season) FROM episodes WHERE series_id = %s"
        cursor.execute(get_seasons, (series_id,))
        seasons = cursor.fetchone()[0]

        series_details = {
            "title": title, 
            "series_id": series_id,
            "seasons": seasons,
            "release_year": release_year, 
            "created_at": created_at
            }

        logger.info(f"{series_details}")

        return series_details

    except (Exception, DatabaseError) as error:
        print_exc()

    finally:
        if conn is not None:
            conn.close()