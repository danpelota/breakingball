import config
import datetime as dt
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, String, Date, Numeric, DateTime, Boolean

Base = declarative_base()
engine = create_engine(config.DB_URL, echo=True)
Session = sessionmaker(bind=engine)


class Game(Base):
    __tablename__ = 'games'
    game_id = Column(String, primary_key=True)
    game_date = Column(Date, nullable=False)
    game_datetime = Column(DateTime)
    season = Column(Integer, nullable=False)
    venue = Column(String, nullable=False, default='')
    game_type = Column(String, nullable=False, default='')
    inning = Column(Integer)
    outs = Column(Integer)
    top_inning = Column(Boolean)
    status = Column(String, nullable=False, default='')
    home_team_id = Column(Integer, nullable=False)
    away_team_id = Column(Integer, nullable=False)
    home_team_runs = Column(Integer)
    away_team_runs = Column(Integer)
    home_team_hits = Column(Integer)
    away_team_hits = Column(Integer)
    home_team_errors = Column(Integer)
    away_team_errors = Column(Integer)
    url = Column(String, nullable=False)


class Team(Base):
    __tablename__ = 'teams'
    team_id = Column(Integer, primary_key=True)
    season = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, default='')
    short_name = Column(String, nullable=False, default='')
    league = Column(String, nullable=False, default='')
    division = Column(String, nullable=False, default='')


class TeamStats(Base):
    __tablename__ = 'team_stats'
    game_id = Column(String, primary_key=True)
    team_id = Column(Integer, primary_key=True)
    wins = Column(Integer)
    losses = Column(Integer)
    batting_avg = Column(Numeric)
    ab = Column(Integer)
    r = Column(Integer)
    h = Column(Integer)
    d = Column(Integer)
    hr = Column(Integer)
    rbi = Column(Integer)
    bb = Column(Integer)
    po = Column(Integer)
    da = Column(Integer)
    so = Column(Integer)
    lob = Column(Integer)
    era = Column(Numeric)
    games_back = Column(Numeric)
    games_back_wildcard = Column(Numeric)


class Pitchers(Base):
    __tablename__ = 'pitchers'
    game_id = Column(String, primary_key=True)
    team_id = Column(Integer, primary_key=True)
    pitcher_id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    full_name = Column(String, nullable=False)
    pos = Column(String, nullable=False)
    out = Column(Integer)
    bf = Column(Integer)
    hr = Column(Integer)
    bb = Column(Integer)
    er = Column(Integer)
    r = Column(Integer)
    h = Column(Integer)
    w = Column(Integer)
    l = Column(Integer)
    era = Column(Numeric)
    np = Column(Integer)
    s = Column(Integer)
    game_score = Column(Integer) # Not sure what this is
    # TODO: Add data from pitchers/*.xml, including WHIP


#     def __repr__(self):
#         return "<Game(game_id='{}', url='{}')>".format(self.game_id, self.url)
