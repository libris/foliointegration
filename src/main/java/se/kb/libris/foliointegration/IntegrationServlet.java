package se.kb.libris.foliointegration;

import org.sqlite.SQLiteConfig;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Instant;

public class IntegrationServlet extends HttpServlet {

    private final String intro = """
                        <!DOCTYPE html>
                        <html lang="en">
                          <head>
                            <meta charset="utf-8">
                            <title>LibrisXL/FOLIO integration</title>
                          </head>
                          <body>
                            <center>
                              <div style="border:1px solid black; margin:25px 50px 75px 100px">
                        """.stripIndent();
    private final String outro = """
                              </div>
                            </center>
                          </body>
                        </html>
                        """.stripIndent();

    protected void doPost(HttpServletRequest request, HttpServletResponse response) {
        response.setStatus(200);
    }

    protected void doGet(HttpServletRequest request, HttpServletResponse response) {

        // Get a read-only database connection. This is separate from the singular connection used by the syncing code,
        // and MUST NOT ever issue writes (see Storage.getConnection for details).
        String dbPath = "/data/libris.sqlite3";
        if (System.getProperty("DBPATH") != null)
            dbPath = System.getProperty("DBPATH");
        var url = "jdbc:sqlite:" + dbPath;
        Connection readOnlyConnection;
        try {
            SQLiteConfig config = new SQLiteConfig();
            config.setReadOnly(true);
            readOnlyConnection = DriverManager.getConnection(url, config.toProperties());
        } catch (SQLException e) {
            try { response.getOutputStream().write("Could not read state.".getBytes(StandardCharsets.UTF_8)); } catch (IOException ioe) { /*ignore*/ }
            response.setStatus(500);
            return;
        }

        try {
            var majorState = Storage.getApplicationState(readOnlyConnection);
            switch (majorState) {
                case INITIAL_LOAD_FROM_LIBRIS:
                    renderLibrisLoad(response.getOutputStream(), readOnlyConnection);
                    break;
                case STAYING_IN_SYNC:
                    renderStayingInSync(response.getOutputStream(), readOnlyConnection);
                    break;
            }
        } catch (IOException ioe) {
            log("HTTP io failure.", ioe);
            response.setStatus(500);
            return;
        }
        response.setStatus(200);
    }

    private void renderStayingInSync(OutputStream os, Connection readOnlyConnection) throws IOException {
        os.write(intro.getBytes(StandardCharsets.UTF_8));

        // EMM sync state
        {
            String untilString = Storage.getState(EmmSync.SYNCED_UNTIL_KEY, readOnlyConnection);
            if (untilString != null) {
                long syncedUntil = Long.parseLong(untilString);
                long now = Instant.now().toEpochMilli();
                long secondDiff = (now - syncedUntil) / 1000;
                String s = "<br/>EMM synced until: " + syncedUntil + ". Meaning " + secondDiff + " seconds behind. <br/><br/>";
                os.write(s.getBytes(StandardCharsets.UTF_8));
            } else {
                String s = "<br/>Odd EMM sync state. Shouldn't happen.<br/>";
                os.write(s.getBytes(StandardCharsets.UTF_8));
            }
        }

        // FOLIO sync state
        {
            String untilString = Storage.getState(FolioSync.SYNCED_UNTIL_KEY, readOnlyConnection);
            if (untilString != null) {
                long syncedUntil = Long.parseLong(untilString);
                long now = Instant.now().toEpochMilli();
                long secondDiff = (now - syncedUntil) / 1000;
                String s = "<br/>FOLIO synced until: " + syncedUntil + ". Meaning " + secondDiff + " seconds behind (or up-to-date if there are no more recent changes). <br/><br/>";
                os.write(s.getBytes(StandardCharsets.UTF_8));
            } else {
                String s = "<br/>Odd FOLIO sync state. Shouldn't happen.<br/>";
                os.write(s.getBytes(StandardCharsets.UTF_8));
            }
        }

        os.write(outro.getBytes(StandardCharsets.UTF_8));
    }

    private void renderLibrisLoad(OutputStream os, Connection readOnlyConnection) throws IOException {
        os.write(intro.getBytes(StandardCharsets.UTF_8));

        String[] sigelList = System.getenv("SIGEL").split(",");

        String downloadingSigel = Storage.getState(EmmDumpImport.DUMP_SIGEL_KEY, readOnlyConnection);
        if (downloadingSigel == null)
            downloadingSigel = "n/a";

        boolean passedActive = false;
        for (int i = 0; i < sigelList.length; ++i) {
            if (sigelList[i].equals(downloadingSigel)) {
                passedActive = true;
                String totalItemsString = Storage.getState(EmmDumpImport.TOTALITEMS_KEY, readOnlyConnection);
                String offsetString = Storage.getState(EmmDumpImport.OFFSET_KEY, readOnlyConnection);
                float progress = 0.0f;
                if (totalItemsString != null && offsetString != null) {
                    int totalItems = Integer.parseInt(totalItemsString);
                    int offset = Integer.parseInt(offsetString);
                    progress = (float) offset / (float) totalItems;
                }
                int percent = (int) (100.0f * progress);
                String s = "<br/>Downloading EMM dump for sigel: " + downloadingSigel + " <br/>" +
                        "<label>Progress:</label>" +
	                    "<progress value=\"" + percent +  "\" max=\"100\"> " + percent +  "% </progress> <br/><br/>";

                os.write(s.getBytes(StandardCharsets.UTF_8));
            } else {
                if (passedActive) {
                    String s = "<br/>Downloading EMM dump for sigel: " + sigelList[i] + " <br/>" +
                            "<label>Progress:</label>" +
                            "<progress value=\"0\" max=\"100\"> 0% </progress> <br/><br/>";
                    os.write(s.getBytes(StandardCharsets.UTF_8));
                } else {
                    String s = "<br/>Downloading EMM dump for sigel: " + sigelList[i] + " <br/>" +
                            "<label>Progress:</label>" +
                            "<progress value=\"100\" max=\"100\"> 100% </progress> <br/><br/>";
                    os.write(s.getBytes(StandardCharsets.UTF_8));
                }
            }
        }

        os.write(outro.getBytes(StandardCharsets.UTF_8));
    }
}
