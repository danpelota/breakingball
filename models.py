import config
import datetime as dt
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, String, Date, Numeric, DateTime

Base = declarative_base()
engine = create_engine(config.DB_URL, echo=True)
Session = sessionmaker(bind=engine)


class Game(Base):
    __tablename__ = 'games'
    game_id = Column(String, primary_key=True)
    game_date = Column(Date, nullable=False)
    game_datetime = Column(DateTime)
    game_type = Column(String, nullable=False, default='')
    venue = Column(String, nullable=False, default='')
    url = Column(String, nullable=False)
    inning = Column(Integer)
    home_games_back = Column(Numeric)


    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.game_date = gid_to_date(self.game_date)
        self.url = gid_to_url(self.game_date)

    def __repr__(self):
        return "<Game(game_id='{}', url='{}')>".format(self.game_id, self.url)
