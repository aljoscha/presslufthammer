#!/usr/bin/env sh

# create 4 tablets containing the same data for now ...
java -cp presslufthammer-1.0-SNAPSHOT-jar-with-dependencies.jar de.tuberlin.dima.presslufthammer.util.Columnarizer Documents.json Document.proto $1 0
java -cp presslufthammer-1.0-SNAPSHOT-jar-with-dependencies.jar de.tuberlin.dima.presslufthammer.util.Columnarizer Documents.json Document.proto $1 1
java -cp presslufthammer-1.0-SNAPSHOT-jar-with-dependencies.jar de.tuberlin.dima.presslufthammer.util.Columnarizer Documents.json Document.proto $1 2
java -cp presslufthammer-1.0-SNAPSHOT-jar-with-dependencies.jar de.tuberlin.dima.presslufthammer.util.Columnarizer Documents.json Document.proto $1 3
