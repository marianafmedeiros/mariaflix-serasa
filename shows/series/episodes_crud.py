from psycopg2 import connect, DatabaseError
from database.database_connection import db_connect
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


def update_episode(season, episode_number, series_release_year=None, series_id=None, series_title=None, new_season=None, new_episode_number=None, new_episode_length=None, new_release_date=None, new_episode_title=None):
    # Passed arguments to update episode
    passed_args = locals()
    del passed_args["season"]
    del passed_args["episode_number"]
    del passed_args["series_release_year"]
    del passed_args["series_id"]
    del passed_args["series_title"]
    
    try:
        conn = db_connect()
        cursor = conn.cursor()
        
        if not series_id:
            # Retrieve series_id
            retrieve_series = "SELECT series_id FROM series WHERE series_title = %s AND release_year = %s"
            cursor.execute(retrieve_series, (series_title,series_release_year))
            series_id = cursor.fetchone()
        
            if not series_id:
                logging.warning("You need to pass a series_id or series_title with a series_release_year")
                return
            else:
                series_id = series_id[0]

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


def update_series(series_id, new_series_title=None, new_release_year=None):
    # Passed arguments to update episode
    passed_args = locals()
    del passed_args["series_id"]

    try:
        conn = db_connect()
        cursor = conn.cursor()

        
        # Check if it exists
        retrieve_series = "SELECT series_title, release_year FROM series WHERE series_id = %s"
        cursor.execute(retrieve_series, (series_id,))
        result = cursor.fetchone()
        EXISTS = result is not None
        
        if not EXISTS:
            logger.info(f"There is no series with id {series_id}")
            return
        
        else:
            print(result)
            series_title, release_year = result
            current_series = [series_title, release_year]
        
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


def delete_episode(series_title, series_release_year, season, episode_number, series_id=None):
    try:
        conn = db_connect()
        cursor = conn.cursor()

        if not series_id:
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


def delete_series(series_title, series_release_year, series_id=None):
    try:
        conn = db_connect()
        cursor = conn.cursor()

        if not series_id:
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


def retrieve_episode(series_title=None, series_release_year=None, season=None, episode_number=None, series_id=None):
    try:
        conn = db_connect()
        cursor = conn.cursor()

        if series_title and series_release_year and season and episode_number:
            if not series_id:
                # Retrieve series_id
                retrieve_series = "SELECT series_id FROM series WHERE series_title = %s AND release_year = %s"
                cursor.execute(retrieve_series, (series_title, series_release_year))
                series_id = cursor.fetchone()[0]

            retrieve_episode = "SELECT episode_length, release_date, show_id FROM episodes WHERE series_id = %s AND season = %s AND episode_number = %s"
            cursor.execute(retrieve_episode, (series_id, season, episode_number))
            episode_length, release_date, show_id = cursor.fetchone()
            release_date = datetime.strftime(release_date, "%d/%m/%Y")

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
                "release_date": release_date
                }

            logger.info(f"{episode_details}")

            return episode_details
        else:
            retrieve_episodes = '''SELECT
                e.episode_number,
                e.season, 
                e.episode_length, 
                e.release_date, 
                e.show_id,
                e.series_id,
                s.series_title 
                FROM episodes e
                LEFT JOIN series s ON s.series_id = e.series_id'''
            cursor.execute(retrieve_episodes)
            results = cursor.fetchall()

        if results:
            all_episodes = [{"episode_number": result[0], 
                "season": result[1], 
                "episode_length": result[2], 
                "release_date": datetime.strftime(result[3],"%d/%m/%Y"), 
                "show_id": result[4], 
                "series_id": result[5], 
                "series_title": result[6]} 
                for result in results]
            
            logger.info(all_episodes)
            return all_episodes
            
    except (Exception, DatabaseError) as error:
        print_exc()

    finally:
        if conn is not None:
            conn.close()


def retrieve_series(series_id=None):
    try:
        conn = db_connect()
        cursor = conn.cursor()

        if series_id:
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

            logger.info(series_details)

            return series_details
        else:
            retrieve_series = "SELECT series_id, series_title, release_year FROM series"
            cursor.execute(retrieve_series)
            results = cursor.fetchall()

        if results:
            all_series = [{"series_id": result[0], "series title": result[1], "release year": result[2]} for result in results]
            
            logger.info(all_series)
            return all_series

    except (Exception, DatabaseError) as error:
        print_exc()

    finally:
        if conn is not None:
            conn.close()