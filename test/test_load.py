import unittest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Base, Game, Pitch
import config
from gameloader import GameLoader
import datetime as dt


# TODO: Mock a few game objects with pre-downloaded XML so we can extensively
# test various loading scenarios
# TODO: Add test for gid_2005_03_18_arimlb_colmlb_1 to ensure we increment pitch
# ID where it doesn't exist

class TestGameLoader(unittest.TestCase):
    def setUp(self):
        url = config.DB_TEST_URL
        self.engine = create_engine(url)
        self.sessionmaker = sessionmaker(bind=self.engine)
        Base.metadata.drop_all(self.engine)
        Base.metadata.create_all(self.engine)
        self.newgame = GameLoader('gid_2015_05_09_cinmlb_chamlb_1',
                                  self.sessionmaker)
        self.oldgame = GameLoader('gid_2008_06_08_balmlb_tormlb_1',
                                  self.sessionmaker)
        self.newgame.load()
        self.oldgame.load()

    def test_init(self):
        self.assertEqual(self.newgame.game_date, dt.date(2015, 5, 9))
        self.assertEqual(self.oldgame.game_date, dt.date(2008, 6, 8))
        self.assertEqual(self.newgame.season, 2015)
        self.assertEqual(self.newgame.base_url,
                         ('http://gd2.mlb.com/components/game/mlb/year_2015/'
                          'month_05/day_09/gid_2015_05_09_cinmlb_chamlb_1/'))

    def test_both_games_loaded(self):
        s = self.sessionmaker()
        games = s.query(Game).all()
        self.assertEqual(len(games), 2)
        s.close()

    def test_pitches_loaded(self):
        s = self.sessionmaker()
        pitches = s.query(Pitch).all()
        self.assertTrue(len(pitches) > 200)
        s.close()

    def tearDown(self):
        Base.metadata.drop_all(self.engine)
        self.engine.dispose()

if __name__ == "__main__":
    unittest.main()
