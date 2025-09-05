# LibrisXL/FOLIO-integration

To run locally (for development), assuming a locally running EMM server and XL REST API:
The record "source" must be manually configured in folio, and its GUID then passed here.

`EMM_BASE_URL="http://localhost:8186/" FOLIO_USER="..." FOLIO_PASS="..." OKAPI_URL="https://okapi-folio-snapshot.okd-test.kb.se" OKAPI_TENANT="kbtest1" SIGEL="X,S" SOURCE_GUID="912ecb39-c577-4596-ad4b-0ed8dedc3a33" java -DDBPATH=/tmp/libris.sqlite3 -jar build/libs/foliolibrisintegration.jar`