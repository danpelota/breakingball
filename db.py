#! /usr/bin/env python

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
import config
import argparse
from models import Base

engine = create_engine(config.DB_URL)
Session = sessionmaker(bind=engine)
db_session = scoped_session(Session)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('action')
    args = parser.parse_args()

    if args.action == 'init':
        Base.metadata.create_all(engine)

    if args.action == 'reset':
        conf_message = ("WARNING: Are you sure you want to destory all "
                        "currently loaded game data? Type 'yes' to coninue: ")
        response = input(conf_message)
        if response == 'yes':
            Base.metadata.drop_all(engine)
            Base.metadata.create_all(engine)


