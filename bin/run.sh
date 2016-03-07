#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
${SPARK_HOME}/bin/spark-submit --class "com.artificialclairvoyance.core.ArtificialClairvoyance" --master local[4] ${DIR}/../target/scala-2.10/artificial-clairvoyance_2.10-0.0.1.jar
