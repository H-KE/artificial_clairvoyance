#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
rm -rf ${DIR}/../app/resources/output/nba_players_historical
rm -rf ${DIR}/../app/resources/output/mlb_players_historical
rm -rf ${DIR}/../app/resources/output/nba_players_current
rm -rf ${DIR}/../app/resources/output/mlb_players_current
rm -rf ${DIR}/../app/resources/output/nba_players_predictions
rm -rf ${DIR}/../app/resources/output/mlb_players_predictions
${SPARK_HOME}/bin/spark-submit --packages com.databricks:spark-csv_2.10:1.2.0 --class "com.artificialclairvoyance.core.ArtificialClairvoyance" --master local[4] ${DIR}/../target/scala-2.10/artificial-clairvoyance_2.10-0.0.1.jar
python ${DIR}/../src/test/python/aggregate/aggregate_parts.py
