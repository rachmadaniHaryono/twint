import itertools
import logging
import os
import sqlite3

import pytest
import vcr

import twint

'''
Test.py - Testing TWINT to make sure everything works.
'''
logging.basicConfig()
vcr_log = logging.getLogger("vcr")
vcr_log.setLevel(logging.INFO)
LOGGER = logging.getLogger(__name__)


@pytest.mark.skip()
def test_reg(c, run):
    LOGGER.info("[+] Beginning vanilla test in {}".format(str(run)))
    run(c)


@pytest.mark.skip()
def test_db(c, run, path=None):
    LOGGER.info("[+] Beginning DB test in {}".format(str(run)))
    if path is None:
        c.Database = "test_twint.db"
    else:
        c.Database = str(path)
    run(c)


@pytest.mark.skip()
def custom(c, run, _type):
    LOGGER.info("[+] Beginning custom {} test in {}".format(_type, str(run)))
    c.Custom['tweet'] = ["id", "username"]
    c.Custom['user'] = ["id", "username"]
    run(c)


@pytest.mark.skip()
def test_json(c, run):
    c.Store_json = True
    c.Output = "test_twint.json"
    custom(c, run, "JSON")
    LOGGER.info("[+] Beginning JSON test in {}".format(str(run)))
    run(c)


@pytest.mark.skip()
def test_csv(c, run):
    c.Store_csv = True
    c.Output = "test_twint.csv"
    custom(c, run, "CSV")
    LOGGER.info("[+] Beginning CSV test in {}".format(str(run)))
    run(c)


@pytest.mark.vcr
@pytest.mark.parametrize(
    'run_tuple, test', itertools.product([
        (twint.run.Profile, 0),  # this doesn't
        (twint.run.Search, 1),  # this works
        (twint.run.Following, 2),
        (twint.run.Followers, 3),
        (twint.run.Favorites, 4),
    ], [test_reg, test_json, test_csv, test_db])
)
def test_main(run_tuple, test):
    c = twint.Config()
    c.Username = "verified"
    c.Limit = 20
    c.Store_object = True
    c.debug = True

    # Separate objects are necessary.

    f = twint.Config()
    f.Username = "verified"
    f.Limit = 20
    f.Store_object = True
    f.User_full = True
    f.debug = True

    # Something breaks if we don't split these up

    run, idx = run_tuple
    if idx < 3:
        if run == twint.run.Search:
            c.Since = "2012-1-1 20:30:22"
            c.Until = "2017-1-1"
        else:
            c.Since = ""
            c.Until = ""

        test(c, run)

    if idx >= 3:
        test(f, run)

    files = [
        #  "test_twint.db",
        "test_twint.json", "test_twint.csv"]
    for _file in files:
        if os.path.isfile(_file):
            os.remove(_file)

    LOGGER.info("[+] Testing complete!")


def main():
    c = twint.Config()
    c.Username = "verified"
    c.Limit = 20
    c.Store_object = True

    # Separate objects are necessary.

    f = twint.Config()
    f.Username = "verified"
    f.Limit = 20
    f.Store_object = True
    f.User_full = True

    runs = [
        twint.run.Profile,  # this doesn't
        twint.run.Search,  # this works
        twint.run.Following,
        twint.run.Followers,
        twint.run.Favorites,
    ]

    tests = [test_reg, test_json, test_csv, test_db]

    # Something breaks if we don't split these up

    for run in runs[:3]:
        if run == twint.run.Search:
            c.Since = "2012-1-1 20:30:22"
            c.Until = "2017-1-1"
        else:
            c.Since = ""
            c.Until = ""

        for test in tests:
            test(c, run)

    for run in runs[3:]:
        for test in tests:
            test(f, run)

    files = ["test_twint.db", "test_twint.json", "test_twint.csv"]
    for _file in files:
        os.remove(_file)

    print("[+] Testing complete!")


@pytest.mark.vcr
def test_db_retweet(tmp_path):
    co = twint.Config()
    co.Username = "BBCBreaking"
    co.Store_object = True
    co.Since = "2020-11-22"
    co.Until = "2020-11-24"
    db_path = str(tmp_path / "test.db")
    test_db(co, twint.run.Profile, db_path)
    conn = sqlite3.connect(db_path)
    cu = conn.cursor()
    cu.execute('select * from retweets')
    assert cu.fetchall() == [(
        265902729, 
        'BBC Sport',
        1330888768748986369, 
        1330887307717701632, 
        1606143235)]


if __name__ == '__main__':
    main()
