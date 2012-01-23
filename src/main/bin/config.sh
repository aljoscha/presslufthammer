#!/usr/bin/env bash

# Resolve links
this="$0"
while [ -h "$this" ]; do
  ls=`ls -ld "$this"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '.*/.*' > /dev/null; then
    this="$link"
  else
    this=`dirname "$this"`/"$link"
  fi
done

# convert relative path to absolute path
bin=`dirname "$this"`
script=`basename "$this"`
bin=`cd "$bin"; pwd`
this="$bin/$script"

# define JAVA_HOME if it is not already set
if [ -z "${JAVA_HOME+x}" ]; then
        JAVA_HOME=/usr/lib/jvm/java-6-openjdk/
fi

# define HOSTNAME if it is not already set
if [ -z "${HOSTNAME+x}" ]; then
        HOSTNAME=`hostname`
fi


# define the main directory of the installation
ROOT_DIR=`dirname "$this"`/..
BIN_DIR=$ROOT_DIR/bin
LIB_DIR=$ROOT_DIR/lib

# default classpath 
CLASSPATH=$( echo $LIB_DIR/*.jar . | sed 's/ /:/g' )
