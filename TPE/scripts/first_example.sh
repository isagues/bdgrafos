#!/bin/bash

JARS=hdfs:///user/isagues/tpe/blueprints-core-2.6.0.jar
JARS=$JARS,hdfs:///user/isagues/graphframes-0.8.0-spark2.4-s_2.11.jar  

CLASS_NAME=ar.edu.itba.graph.AirRoutesMain
MAIN_JAR=hdfs:///user/isagues/tpe/original-tpe-isagues-1.jar 
ARGS=hdfs:///user/isagues/tpe/first-example.graphml

spark-submit --master yarn --deploy-mode cluster --jars=$JARS --class $CLASS_NAME $MAIN_JAR $ARGS
