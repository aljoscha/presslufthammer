#!/usr/bin/env bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

$JAVA_HOME/bin/java -classpath $CLASSPATH de.tuberlin.dima.presslufthammer.transport.CLIClient $1 $2
