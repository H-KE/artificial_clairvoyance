#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
rm -r ${DIR}/../app/resources/output/nba_players_historical2
rm -r ${DIR}/../app/resources/output/mlb_players_historical2
rm -r ${DIR}/../app/resources/output/nba_players_current2
rm -r ${DIR}/../app/resources/output/mlb_players_current2
${SPARK_HOME}/bin/spark-submit --packages com.databricks:spark-csv_2.10:1.2.0 --class "com.artificialclairvoyance.core.ArtificialClairvoyance" --master local[4] ${DIR}/../target/scala-2.10/artificial-clairvoyance_2.10-0.0.1.jar
