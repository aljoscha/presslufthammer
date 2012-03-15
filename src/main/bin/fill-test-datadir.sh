#!/usr/bin/env bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

$JAVA_HOME/bin/java -classpath $CLASSPATH de.tuberlin.dima.presslufthammer.util.Columnarizer documents.json Document.proto $1 2
$JAVA_HOME/bin/java -classpath $CLASSPATH de.tuberlin.dima.presslufthammer.util.Columnarizer sentences-reducedPunctuation.json Sentence.proto $1 1000
