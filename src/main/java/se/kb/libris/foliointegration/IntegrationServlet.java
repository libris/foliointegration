package se.kb.libris.foliointegration;

import org.sqlite.SQLiteConfig;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class IntegrationServlet extends HttpServlet {
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


        String test = System.getenv("ENVTEST");
        try {
            if (test != null) {
                response.getOutputStream().write(test.getBytes(StandardCharsets.UTF_8));
            }
        } catch (IOException e) {
            Storage.log("bla bla", e);
        }
        response.setStatus(200);
    }

    protected void doPost(HttpServletRequest request, HttpServletResponse response) {
        response.setStatus(200);
    }
}
