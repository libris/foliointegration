# LibrisXL/FOLIO-integration

This application is intended to be used as a synchronization mechanism between LibrisXL and an instance of FOLIO.

Generally, this application will periodically poll a LibrisXL EMM API endpoint, and write the corresponding changes it finds there
into FOLIO. Since EMM publishes only change-notifications, this application will also be making requests to LibrisXL's
CRUD API for the actual data.

## Application life-cycle
When this application starts it checks for the existence of its state file. By default this is assumed to be `/data/libris.sqlite3`
but if necessary you can control this with a Java system property (not environment variable) for example like so: `-DDBPATH=/tmp/libris.sqlite3`.
If no state file exists at the designated path, one is created and the application will then immediately enter a dump-downloading
state. Full dumps for all of your selected SIGEL codes will be downloaded, one by one. Only when these dumps have been fully
downloaded will synchronization with FOLIO begin.

The synchronization actually consists of two separate mechanisms. First there is the synchronization of Libris-data into
the state file of this application (which can grow quite large). Second there is the synchronization between the internal
state of this application and FOLIO. This may seem confusing, but is useful as there is no 1:1 correspondence between a
change in Libris and a change in folio, nor for that part between a record in Libris and a record in FOLIO. This application
keeps track of what needs to be updated in FOLIO due to any given change in Libris.
Both of these mechanisms use a point-in-time to know what has already been done, and what is yet to be done.

These points are essentially:
1. "The last update we successfully had updated from Libris, when was it written in Libris?"
2. "The last update we successfully wrote into FOLIO, when was it written into our own state file?"

All changes (both from libris to this application and from this application into FOLIO) are written strictly in chronological
order. Both of these timestamps can be changed dynamically from the GUI (served at port 8484 on the machine running this
application), which lets you control for example if some set of changes  should be written again, or some other set of
changes should be skipped.

The application never leaves the synchronization state once entered. Closing/killing/restarting the application can be done
freely at any time (including during dump-downloading).


## Runtime parameters
This application requires a number of runtime parameters, passed to the application in the form of environment variables.

`
EMM_BASE_URL
`
This is URL on which Libris serves the EMM API. Generally, this should be set to: `https://libris.kb.se/api/emm/` Although
for testing/experimentation, please use `https://libris-qa.kb.se/api/emm/`

`
FOLIO_USER
`
This is the username that the application should use to log into folio.

`
FOLIO_PASS
`
This is the password for the above user.

`
OKAPI_URL
`
This is the base URL of the FOLIO instance to be kept up to date.

`
OKAPI_TENANT
`
A FOLIO paremeter, please refer to FOLIO documentation.

`
SIGEL
`
The (comma-separated) list of SIGEL codes (library identification codes) to write to FOLIO. Libris contains overlapping data for
many libraries. This parameters tells the application what parts of the data in libris to write to this particular FOLIO instance

`
FOLIO_WRITE_BATCH_SIZE
`
This application can, if there is a need, throttle the writes it does to FOLIO. This parameter specifies how many records
should be sent in each batch. Do *not* set this higher than `20` as that will typically cause errors in FOLIO. 

`
FOLIO_WRITE_CELL_SECONDS
`
This specifies the "cell" size to use (for throttling writes to FOLIO), in seconds. Generally set this to `1`

`
FOLIO_WRITE_BATCHES_PER_CELL
`
This specifies how many batches of records should be sent (max) to folio for writing in each "cell". If you effectively
want no throttling, set this to a high value, like `100`. The number of records written to FOLIO per second can be
calculated as `FOLIO_WRITE_BATCH_SIZE * FOLIO_WRITE_BATCHES_PER_CELL / FOLIO_WRITE_CELL_SECONDS`.

`
INSTANCE_JSLT_URL
`
This application uses JSLT-files (https://github.com/schibsted/jslt) to convert records into the format that FOLIO wants.
This parameter tells the application where to look for the JSLT file that converts Instances. Generally this check
happens on every update-increment (so think of it as happening on a second basis).

`
ITEM_JSLT_URL
`
This works the same way as the parameter above, but instead specifies where to look for the JSLT file that converts Libris
holdings-records into FOLIO items.


## Development

To run locally (for development), assuming a locally running EMM server and XL REST API:

`EMM_BASE_URL="http://localhost:8186/" FOLIO_USER="..." FOLIO_PASS="..." OKAPI_URL="https://okapi-folio-snapshot.okd-test.kb.se" OKAPI_TENANT="kbtest1" SIGEL="X,S" FOLIO_WRITE_BATCH_SIZE="20" FOLIO_WRITE_BATCHES_PER_CELL="50" FOLIO_WRITE_CELL_SECONDS="1" INSTANCE_JSLT_URL="https://git.kb.se/libris-folio/format-conversion/-/raw/develop/public/instance.jslt" ITEM_JSLT_URL="https://git.kb.se/libris-folio/format-conversion/-/raw/develop/public/item.jslt" java -DDBPATH=/tmp/libris.sqlite3 -jar build/libs/foliolibrisintegration.jar`
