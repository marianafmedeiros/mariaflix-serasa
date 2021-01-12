from shows.movies.movies_crud import add_movie
from shows.series.episodes_crud import add_episode, add_series
from users.users_crud import add_user
from user_show.user_show import start_watching, finish_watching
from datetime import datetime
import logging
import json


logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
logger = logging.getLogger("feed-database")
handler = logging.FileHandler('mylog.log')
# create a logging format
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

def feed_database (intents: set):

    if "movies" in intents:
        with open("./database./feed_database.json") as f:
            data = json.load(f)

        movies = data["movies"]

        for movie in movies:
            movie_title = movie["title"]
            movie_year = movie["year"]
            movie_length = movie["length"]
            add_movie(movie_title,movie_year, movie_length)

    if "series" in intents:
        with open("./database./feed_database.json") as f:
            data = json.load(f)
        
        all_series = data["series"]

        for series in all_series:
            series_title = series["title"]
            series_year = series["year"]
            add_series(series_title, series_year)

            all_episodes = series["episodes"]
            
            for episode in all_episodes:
                title = episode["title"]
                season = episode["season"]
                number = episode["number"]
                release_date = episode["release_date"]
                release_date = datetime.strptime(release_date, "%d/%m/%Y")
                length = episode["length"]
                add_episode(series_title, number, season, title, length, release_date)
    
    if "users" in intents:
        with open("./database./feed_database.json") as f:
            data = json.load(f)

        users = data["users"]

        for user in users:
            user_email = user["email"]
            user_username = user["username"]
            add_user(user_email, user_username)
    
    if "user_show" in intents:
        try:
            with open("./database./feed_database.json") as f:
                data = json.load(f)

            users_shows = data["user_show"]

            for user_show in users_shows:
                start_watching(user_show["user_id"], user_show["show_id"])
                finish_watching(user_show["user_id"], user_show["show_id"])
        except:
            logger.warning("Check if you populated your database with movies, series and users!")

