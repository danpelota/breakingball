from flask import Flask, render_template, url_for
from db import db_session
from models import Game
from sqlalchemy import func

app = Flask(__name__)

@app.teardown_appcontext
def shutdown_session(exception=None):
    db_session.remove()

@app.route('/')
def index():
    game_count = db_session.query(func.count(Game.game_id)).scalar()
    games = db_session.query(Game).order_by(Game.game_date.desc()).limit(10)
    return render_template('index.html', games=games, game_count=game_count)

@app.route('/game_details/<game_id>')
def game_details(game_id):
    game = db_session.query(Game).filter(Game.game_id == game_id).first()
    return render_template('game_details.html', game=game)


if __name__ == "__main__":
    app.run(debug=True)
