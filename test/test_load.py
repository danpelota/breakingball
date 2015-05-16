import datetime as dt
import unittest
from breakingball.load import GameLoader
from breakingball.models import Session
from bs4 import BeautifulSoup
import bs4

class TestGameLoader(unittest.TestCase):
    def setUp(self):
        self.game1_gid = 'gid_2015_04_27_phimlb_slnmlb_1'
        self.sessionmaker = Session
        self.game1 = GameLoader(self.game1_gid, self.sessionmaker)

    def test_init(self):
        self.assertEqual(self.game1.game_id, self.game1_gid)
        self.assertEqual(self.game1.session, self.session)

    def test_fetch_linescore(self):
        self.game1.fetch_linescore()
        self.assertIsInstance(self.game1.linescore_soup, bs4.element.Tag)
        self.assertEqual(self.game1.linescore_soup.get('id'),
                         '2015/04/27/phimlb-slnmlb-1')

    def test_fetch_boxscore(self):
        self.game1.fetch_boxscore()
        self.assertIsInstance(self.game1.boxscore_soup, bs4.element.Tag)
        self.assertEqual(self.game1.boxscore_soup.get('game_id'),
                         '2015/04/27/phimlb-slnmlb-1')

    def test_fetch_innings(self):
        self.game1.fetch_innings()
        self.assertEqual(len(self.game1.innings), 9)



if __name__ == "__main__":
    unittest.main()
