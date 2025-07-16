# LibrisXL/FOLIO-integration

To run locally (for development), assuming a locally running EMM server and XL REST API:

`EMMBASEURL="http://localhost:8186/" FOLIOUSER="..." FOLIOPASS="..." OKAPI_URL="..." OKAPI_TENANT="..." SIGEL="X,S" java -DDBPATH=/tmp/libris.sqlite3 -jar build/libs/foliolibrisintegration.jar`