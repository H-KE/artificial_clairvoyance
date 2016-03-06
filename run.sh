#!/bin/bash

${SPARK_HOME}/bin/spark-submit --class "com.artificialclairvoyance.core.ArtificialClairvoyance" --master local[4] target/scala-2.10/artificial-clairvoyance_2.10-0.0.1.jar
