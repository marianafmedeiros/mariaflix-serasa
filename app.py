from flask import Flask, send_from_directory, request, jsonify
import os
import datetime
from reports.reports import user_movie_report, user_seasons_report, month_top5
from users.users_crud import retrieve_user
from shows.series.episodes_crud import retrieve_series, retrieve_episode
from shows.movies.movies_crud import retrieve_movie

from user_show.user_show import start_watching, finish_watching

# Create temp directory if it does not exist
if not os.path.isdir("temp"):
    os.mkdir("temp")

app = Flask(__name__)

@app.route('/')
def hello():
    return "Hey, thanks for testing my project :)"


#Users

@app.route('/users')
def get_users():
    result = retrieve_user()
    result = jsonify(result)
    return result, 200


@app.route('/users/<int:user_id>/start-show', methods=['POST'])
def user_watch_show(user_id):
    req_data = request.get_json()
    show_id = req_data["show_id"]
    start_watching(user_id, show_id)
    return jsonify({"status": "Successful!"}), 200


@app.route('/users/<int:user_id>/finish-show', methods=['POST'])
def user_finish_show(user_id):
    req_data = request.get_json()
    show_id = req_data["show_id"]
    finish_watching(user_id, show_id)
    return jsonify({"status": "Successful!"}), 200


# Shows

@app.route('/shows/series/episodes')
def get_episodes():
    result = retrieve_episode()
    result = jsonify(result)
    return result, 200


@app.route('/shows/series')
def get_series():
    result = retrieve_series()
    result = jsonify(result)
    return result, 200


@app.route('/shows/movies')
def get_movies():
    result = retrieve_movie()
    result = jsonify(result)
    return result, 200


# Reports

@app.route('/users/<int:user_id>/reports/user-movies')
def report_user_movies(user_id):
    result = user_movie_report(user_id)
    return result, 200



@app.route('/users/<int:user_id>/reports/user-movies/download')
def report_user_movies_download(user_id):
    user_movie_report(user_id)
    from_directory = "./temp"
    return send_from_directory(from_directory, filename="report.xlsx", as_attachment=True )


@app.route('/users/<int:user_id>/reports/user-seasons')
def report_user_seasons(user_id):
    result = user_seasons_report(user_id)
    result = jsonify(result)
    return result, 200


@app.route('/users/<int:user_id>/reports/user-seasons/download')
def report_user_seasons_download(user_id):
    user_seasons_report(user_id)
    from_directory = "./temp"
    return send_from_directory(from_directory, filename="report.xlsx", as_attachment=True ) 


@app.route('/reports/month-top5/<int:year>/<int:month>/')
def report_top5(month, year):
    result = month_top5(month, year)
    return result, 200


@app.route('/reports/month-top5/<int:year>/<int:month>/download')
def report_top5_download(month, year):
    month_top5(month, year)
    from_directory = "./temp"
    return send_from_directory(from_directory, filename="report.xlsx", as_attachment=True ) 
