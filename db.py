from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import config

engine = create_engine(config.DB_URL)
Session = sessionmaker(bind=engine)
