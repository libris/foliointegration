#!/bin/bash
set -e
./gradlew jar
sudo docker build . -t libris/foliointegration
echo "To run locally in container now do: sudo docker run -p 8080:8080 --mount type=volume,src=LibrisFoliosState,dst=/data libris/foliointegration"
echo "To run locally native now do: java -DDBPATH=/tmp/libris.sqlite3 -jar build/libs/foliolibrisintegration.jar"
