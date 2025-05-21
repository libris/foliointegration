#!/bin/bash
set -e
./gradlew jar
sudo docker build . -t libris/foliointegration
echo "To run now do: sudo docker run libris/foliointegration"
