#!/usr/bin/env bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

# create 4 tablets containing the same data for now ...
$JAVA_HOME/bin/java -classpath $CLASSPATH de.tuberlin.dima.presslufthammer.util.Columnarizer documents.json Document.proto $1 0
$JAVA_HOME/bin/java -classpath $CLASSPATH de.tuberlin.dima.presslufthammer.util.Columnarizer documents.json Document.proto $1 1
$JAVA_HOME/bin/java -classpath $CLASSPATH de.tuberlin.dima.presslufthammer.util.Columnarizer documents.json Document.proto $1 2
$JAVA_HOME/bin/java -classpath $CLASSPATH de.tuberlin.dima.presslufthammer.util.Columnarizer documents.json Document.proto $1 3
