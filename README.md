# LibrisXL/FOLIO-integration

To run locally (for development), assuming a locally running EMM server and XL REST API:
The record "source" must be manually configured in folio, and its GUID then passed here.

`EMM_BASE_URL="http://localhost:8186/" FOLIO_USER="..." FOLIO_PASS="..." OKAPI_URL="https://okapi-folio-snapshot.okd-test.kb.se" OKAPI_TENANT="kbtest1" SIGEL="X,S" SOURCE_GUID="912ecb39-c577-4596-ad4b-0ed8dedc3a33" FOLIO_WRITE_BATCH_SIZE="20" FOLIO_WRITE_BATCHES_PER_CELL="50" FOLIO_WRITE_CELL_SECONDS="1" INSTANCE_JSLT_URL="https://git.kb.se/libris-folio/format-conversion/-/raw/develop/public/instance.jslt" ITEM_JSLT_URL="https://git.kb.se/libris-folio/format-conversion/-/raw/develop/public/item.jslt" java -DDBPATH=/tmp/libris.sqlite3 -jar build/libs/foliolibrisintegration.jar`