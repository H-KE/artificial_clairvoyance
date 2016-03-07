#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
mkdir /tmp/artificialclairvoyance
mkdir /tmp/artificialclairvoyance/nba
mkdir /tmp/artificialclairvoyance/mlb
echo Scraping NBA Data...
python ${DIR}/../collector/python/nba_scraper.py
#TODO Fix this:
echo Scraping MLB Data...
cp ${DIR}/../src/test/resources/lahman-csv_2015-01-24/Batting_modified.csv /tmp/artificialclairvoyance/mlb/Batting_modified.csv
#python ${DIR}/../collector/python/mlb_scraper.py
