#!/bin/bash
set -e
./gradlew jar
sudo docker build . -t libris/foliointegration
echo 'To run locally in container now do: sudo docker run -e EMMBASEURL="https://libris-dev.kb.se/api/emm/" -e SIGEL="X,S" -p 8080:8080 --mount type=volume,src=LibrisFoliosState,dst=/data libris/foliointegration'
echo 'To run locally native now do: EMMBASEURL="https://libris-dev.kb.se/api/emm/" SIGEL="X,S" java -DDBPATH=/tmp/libris.sqlite3 -jar build/libs/foliolibrisintegration.jar'
