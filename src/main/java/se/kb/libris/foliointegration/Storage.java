package se.kb.libris.foliointegration;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;

import org.sqlite.SQLiteConfig;

public class Storage {

    private static Connection _connection = null;

    public static void log(String message, Exception e) {
        System.err.println(message);
        e.printStackTrace(System.err);
        System.err.println("---------------");
    }

    private static synchronized void initDb(Connection connection) throws SQLException {
        {
            String sql = """
                    CREATE TABLE entities (
                        id INTEGER PRIMARY KEY,
                        entity TEXT
                    );
                    """.stripIndent();
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.execute();
            }
        }
        {
            String sql = """
                    CREATE TABLE uris (
                          id INTEGER PRIMARY KEY,
                          uri TEXT,
                          entity_id INTEGER,
                          UNIQUE(uri, entity_id) ON CONFLICT IGNORE,
                          FOREIGN KEY (entity_id) REFERENCES entities(id) ON DELETE CASCADE
                      );
                    """.stripIndent();
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.execute();
            }
        }
        {
            String sql = """
                    CREATE TABLE state (
                            id INTEGER PRIMARY KEY,
                            changes_consumed_until TEXT
                        );
                    """.stripIndent();
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.execute();
            }
        }
        {
            String sql = """
                    CREATE INDEX idx_uris_uri ON uris(uri);
                    """.stripIndent();
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.execute();
            }
        }
        {
            String sql = """
                    CREATE INDEX idx_uris_entity_id ON uris(entity_id);
                    """.stripIndent();
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.execute();
            }
        }
    }

    private static synchronized Connection getConnection() {
        if (_connection != null)
            return _connection;

        String dbPath = "/data/libris.sqlite3"; // default, and typically in use when running in container
        if (System.getProperty("DBPATH") != null)
            dbPath = System.getProperty("DBPATH"); // For native running, allow this to be set (typically: -DDBPATH=/tmp)

        boolean preExistingState = Files.exists(Path.of(dbPath));

        var url = "jdbc:sqlite:" + dbPath;
        try {
            SQLiteConfig config = new SQLiteConfig();
            config.setJournalMode(SQLiteConfig.JournalMode.WAL);
            config.setSynchronous(SQLiteConfig.SynchronousMode.OFF);
            config.setPragma(SQLiteConfig.Pragma.FOREIGN_KEYS, "ON");
            _connection = DriverManager.getConnection(url);

            if (!preExistingState) {
                initDb(_connection);
            }

            return _connection;
        } catch (Exception e) {
            log("Did you forget the mount the /data volume? SQLITE3 failure (unrecoverable).", e);
            System.exit(1);
        }
        return null; // can't happen
    }

    public static synchronized void whatever() {
        Connection connection = getConnection();
    }
}
