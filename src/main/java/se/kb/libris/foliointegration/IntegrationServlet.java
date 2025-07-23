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

public class IntegrationServlet extends HttpServlet {

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
                case INITIAL_LOAD_TO_FOLIO:
                    break;
                case STAYING_IN_SYNC:
                    break;
            }
        } catch (IOException ioe) {
            log("HTTP io failure.", ioe);
            response.setStatus(500);
            return;
        }
        response.setStatus(200);
    }

    private void renderLibrisLoad(OutputStream os, Connection readOnlyConnection) throws IOException {

        String intro = """
                        <!DOCTYPE html>
                        <html lang="en">
                          <head>
                            <meta charset="utf-8">
                            <title>LibrisXL/FOLIO integration</title>
                          </head>
                          <body>
                            <center>
                        """.stripIndent();
        os.write(intro.getBytes(StandardCharsets.UTF_8));

        String[] sigelList = System.getenv("SIGEL").split(",");

        String downloadingSigel = Storage.getState(EmmDumpImport.DUMP_SIGEL_KEY, readOnlyConnection);
        if (downloadingSigel == null)
            downloadingSigel = "n/a";

        for (int i = 0; i < sigelList.length; ++i) {
            if (sigelList[i].equals(downloadingSigel)) {
                String totalItemsString = Storage.getState(EmmDumpImport.TOTALITEMS_KEY, readOnlyConnection);
                String offsetString = Storage.getState(EmmDumpImport.OFFSET_KEY, readOnlyConnection);
                float progress = 0.0f;
                if (totalItemsString != null && offsetString != null) {
                    int totalItems = Integer.parseInt(totalItemsString);
                    int offset = Integer.parseInt(offsetString);
                    progress = (float) offset / (float) totalItems;
                }

                String s = "Currently loading dump: " + downloadingSigel + ": " + 100.0f*progress + "%";
                os.write(s.getBytes(StandardCharsets.UTF_8));
            } else {
                String s = sigelList[i] + "<br/>";
                os.write(s.getBytes(StandardCharsets.UTF_8));
            }
        }

        String outro = """
                            </center>
                          </body>
                        </html>
                        """.stripIndent();
        os.write(outro.getBytes(StandardCharsets.UTF_8));
    }
}
