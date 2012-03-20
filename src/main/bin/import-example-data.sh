#!/usr/bin/env bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

bin/import-data.sh Document.proto documents.json 2 $1
bin/import-data.sh Sentence.proto sentences-reducedPunctuation.json 1000 $1
