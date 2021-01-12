from psycopg2 import connect, DatabaseError
from database.database_connection import db_connect
import pandas as pd
import logging
import os
from datetime import datetime
from traceback import print_exc

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
logger = logging.getLogger("user-show")
handler = logging.FileHandler('mylog.log')
# create a logging format
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

def user_movie_report(user_id):
    try:
        conn = db_connect()
        cursor = conn.cursor()

        get_show_ids = "SELECT show_id FROM user_show WHERE user_id = %s and finished_at IS NOT NULL" 
        cursor.execute(get_show_ids, (user_id, ))
        show_ids = cursor.fetchall()

        show_id_list = [show_id[0] for show_id in show_ids]
        show_id_tuple = tuple(show_id_list)
        get_movies = '''SELECT title FROM shows WHERE show_type = 1 AND show_id IN %s'''
        cursor.execute(get_movies, (show_id_tuple, ))
        movies = cursor.fetchall()
        movies = [element[0] for element in movies]
        
        movies_df = pd.DataFrame({"Movies You Watched": movies})
        download_path = "./temp/report.xlsx"
        movies_df.to_excel(download_path, index=False)
        

        conn.commit()
        logger.info(f"User {user_id} watched: {movies}")

        movies = {"Movies watched": movies}
        print(movies)
        return movies   

    except (Exception, DatabaseError) as error:
        print_exc()

    finally:
        if conn is not None:
            conn.close()


def user_seasons_report(user_id):
    try:
        conn = db_connect()
        cursor = conn.cursor()

        get_show_ids = "SELECT show_id FROM user_show WHERE user_id = %s and finished_at IS NOT NULL" 
        cursor.execute(get_show_ids, (user_id, ))
        results = cursor.fetchall()
        watched_show_ids = [result[0] for result in results]
        watched_show_ids = set(watched_show_ids)

        # Get series_id, season and title for the watched episodes
        series_season_set = set()
        for show_id in watched_show_ids:
            get_series_id_from_show_id = '''SELECT e.series_id, e.season, s.series_title 
                FROM episodes e
                LEFT JOIN series s ON s.series_id = e.series_id
                WHERE show_id = %s'''
            cursor.execute(get_series_id_from_show_id, (show_id, ))
            results = cursor.fetchone()
            if results:
                result_series_id, result_series_season, result_series_title = results
            else:
                continue
            
            # Using set so we do not get duplicate entries
            series_season_set.add((result_series_id, result_series_season, result_series_title))
        
        # Get all episodes from each watched season and series, see if user watched
        watched_complete_seasons = []
        for series in series_season_set:
            series_id, season, series_title = series
            get_episodes_from_series_season = "SELECT show_id FROM episodes where season = %s and series_id = %s"
            cursor.execute(get_episodes_from_series_season, (season, series_id))
            results = cursor.fetchall()
            season_show_ids = [result[0] for result in results]
            season_show_ids = set(season_show_ids)

            # Check if user watched all episodes from the season
            if season_show_ids.issubset(watched_show_ids):
                watched_complete_seasons.append({"series_title": series_title, "watched_season": season})
            
        if watched_complete_seasons:
            watched_seasons_df = pd.DataFrame(watched_complete_seasons)
            download_path = "./temp/report.xlsx"
            watched_seasons_df.to_excel(download_path, index=False)

        
        logger.info(f"User {user_id}: {watched_complete_seasons}")
        return watched_complete_seasons

        conn.commit()

    except (Exception, DatabaseError) as error:
        print_exc()

    finally:
        if conn is not None:
            conn.close()


def month_top5(month, year):
    try:
        conn = db_connect()
        cursor = conn.cursor()

        get_users_count_from_month_year = '''WITH temp_table as (SELECT 
					    EXTRACT(MONTH FROM started_at) as month, 
					    EXTRACT(YEAR FROM started_at) as year, 
					    user_id
	                    FROM user_show
	                    WHERE finished_at IS NOT NULL
                        )
                        SELECT COUNT(*), user_id FROM temp_table
                        WHERE month = %s AND year = %s
                        GROUP BY user_id
                        ORDER BY 1 DESC'''

        cursor.execute(get_users_count_from_month_year, (month, year ))
        results = cursor.fetchmany(5)
        most_active_users_ids = tuple([user[1] for user in results])

        get_users = '''SELECT username FROM users WHERE user_id IN %s'''
        cursor.execute(get_users, ( most_active_users_ids, ))
        users = cursor.fetchall()
        users = [element[0] for element in users]
        
        users = {"Top 5 active users": users}
        users_df = pd.DataFrame(users)
        download_path = "./temp/report.xlsx"
        users_df.to_excel(download_path, index=False)
        

        conn.commit()
        logger.info(f"{users}")
        return users


    except (Exception, DatabaseError) as error:
        print_exc()

    finally:
        if conn is not None:
            conn.close()