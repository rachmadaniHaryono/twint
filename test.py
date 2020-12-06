import itertools
import json
import logging
import os
import shutil
import sqlite3
import typing

import pytest
import vcr
import yaml

import twint

"""
Test.py - Testing TWINT to make sure everything works.
"""
logging.basicConfig()
vcr_log = logging.getLogger("vcr")
vcr_log.setLevel(logging.INFO)
LOGGER = logging.getLogger(__name__)
JSON_TEST_FILE = "test_twint.json"
DB_TEST_FILE = "test_twint.db"
CSV_TEST_FILE = "test_twint.csv"


@pytest.mark.skip()
def test_reg(c, run):
    LOGGER.info("[+] Beginning vanilla test in {}".format(str(run)))
    run(c)


@pytest.mark.skip()
def test_db(c: twint.Config, run: typing.Callable, path: str = DB_TEST_FILE):
    LOGGER.info("[+] Beginning DB test in {}".format(str(run)))
    c.Database = path
    run(c)


@pytest.mark.skip()
def custom(c, run, _type):
    LOGGER.info("[+] Beginning custom {} test in {}".format(_type, str(run)))
    c.Custom["tweet"] = ["id", "username"]
    c.Custom["user"] = ["id", "username"]
    run(c)


@pytest.mark.skip()
def test_json(c, run):
    c.Store_json = True
    c.Output = JSON_TEST_FILE
    custom(c, run, "JSON")
    LOGGER.info("[+] Beginning JSON test in {}".format(str(run)))
    run(c)


@pytest.mark.skip()
def test_csv(c, run):
    c.Store_csv = True
    c.Output = CSV_TEST_FILE
    custom(c, run, "CSV")
    LOGGER.info("[+] Beginning CSV test in {}".format(str(run)))
    run(c)


@pytest.mark.freeze_time("2020-12-21")
@pytest.mark.vcr
@pytest.mark.parametrize(
    "run_tuple, test",
    itertools.product(
        [
            (twint.run.Profile, 0),  # this doesn't
            (twint.run.Search, 1),  # this works
            (twint.run.Following, 2),
            (twint.run.Followers, 3),
            (twint.run.Favorites, 4),
        ],
        [test_reg, test_json, test_csv, test_db],
    ),
)
def test_main(run_tuple, test, request):
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

    if test == test_json:
        res_file_path = "{}.json".format(request.node.name)
        if os.path.isfile(res_file_path):
            with open(JSON_TEST_FILE) as f1, open(res_file_path) as f2:
                res = sorted(set(f1.read().splitlines()))
                exp_res = sorted(set(f2.read().splitlines()))
                assert res == exp_res
        elif not os.path.isfile(JSON_TEST_FILE):
            LOGGER.debug("{} is not exist on test_json".format(JSON_TEST_FILE))
        else:
            shutil.copyfile(JSON_TEST_FILE, res_file_path)
            LOGGER.info(
                "file copied from {} to {}".format(JSON_TEST_FILE, res_file_path)
            )
    if test == test_db:
        # NOTE: dump to yaml to keep tuple
        res_file_path = "{}.yaml".format(request.node.name)
        con = sqlite3.connect(DB_TEST_FILE)
        cursor = con.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [x[0] for x in cursor.fetchall()]
        obj_data = {}
        for table in tables:
            cursor.execute("SELECT * from {}".format(table))
            obj_data[table] = cursor.fetchall()
        if os.path.isfile(res_file_path):
            with open(res_file_path) as f:
                exp_obj_data = yaml.load(f)
            # NOTE: sort on following_names
            if "following_names" in exp_obj_data:
                obj_data["following_names"] = list(sorted(obj_data["following_names"]))
                exp_obj_data["following_names"] = list(
                    sorted(exp_obj_data["following_names"])
                )
            assert obj_data == exp_obj_data
        else:
            with open(res_file_path, "w") as f:
                yaml.dump(obj_data, f)
    if test == test_csv:
        res_file_path = "{}.csv".format(request.node.name)
        if os.path.isfile(res_file_path):
            with open(CSV_TEST_FILE) as f1, open(res_file_path) as f2:
                assert f1.read() == f2.read()
        elif not os.path.isfile(CSV_TEST_FILE):
            LOGGER.debug("{} is not exist on test_csv".format(CSV_TEST_FILE))
        else:
            shutil.copyfile(CSV_TEST_FILE, res_file_path)
            LOGGER.info(
                "file copied from {} to {}".format(CSV_TEST_FILE, res_file_path)
            )

    files = [DB_TEST_FILE, JSON_TEST_FILE, CSV_TEST_FILE]
    for _file in files:
        if os.path.isfile(_file):
            os.remove(_file)

    LOGGER.info("[+] Testing complete!")


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
    cu.execute("select * from retweets")
    assert cu.fetchall() == [
        (265902729, "BBC Sport", 1330888768748986369, 1330887307717701632, 1606143235)
    ]


if __name__ == "__main__":
    main()
