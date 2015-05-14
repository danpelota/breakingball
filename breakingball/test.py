from models import Session
from load import GameLoader

session = Session()

game_ids = ['gid_2008_03_21_clemlb_atlmlb_1',
            'gid_2008_04_24_clemlb_kcamlb_1',
            'gid_2008_04_24_clemlb_kcamlb_2',
            'gid_2011_05_07_detmlb_tormlb_1',
            'gid_2014_10_17_kcamlb_balmlb_1',
            'gid_2014_10_22_nlcmlb_alcmlb_1',
            'gid_2014_10_22_sfnmlb_kcamlb_1',
            'gid_2015_05_11_kcamlb_texmlb_1', ]

for game_id in game_ids:
    g = GameLoader(game_id, session)
    g.load(False)
