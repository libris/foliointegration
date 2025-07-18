package se.kb.libris.foliointegration;

import org.eclipse.jetty.ee8.servlet.ServletContextHandler;
import org.eclipse.jetty.ee8.servlet.ServletHolder;
import org.eclipse.jetty.server.*;
import org.eclipse.jetty.util.thread.ExecutorThreadPool;

import java.sql.Connection;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

public class Server {

    private static org.eclipse.jetty.server.Server createServer() {
        int maxConnections = 50;
        var queue = new ArrayBlockingQueue<Runnable>(maxConnections);
        var pool = new ExecutorThreadPool(maxConnections, maxConnections, queue);

        var server = new org.eclipse.jetty.server.Server(pool);

        int port = 8484;

        var httpConfig = new HttpConfiguration();
        httpConfig.setIdleTimeout(5 * 60 * 1000); // more than nginx keepalive_timeout
        httpConfig.setPersistentConnectionsEnabled(true);
        httpConfig.setCustomizers(List.of(new ForwardedRequestCustomizer()));

        try (var http = new ServerConnector(server, new HttpConnectionFactory(httpConfig))) {
            http.setPort(port);
            http.setAcceptQueueSize(maxConnections);
            server.setConnectors(new Connector[]{ http });
            System.err.println("Started server on port " + port);
        }

        server.addBean(new ConnectionLimit(maxConnections, server));

        return server;
    }

    public static void main(String[] args) throws Exception {

        //FolioWriting.testWrite();

        var server = createServer();
        ServletContextHandler context = new ServletContextHandler();
        context.setContextPath("/");

        server.setHandler(context);

        ServletHolder holder = new ServletHolder(IntegrationServlet.class);
        holder.setInitOrder(0);
        context.addServlet(holder, "/");

        // Set the initial state if needed
        {
            Connection connection = Storage.getConnection();
            Storage.APPLICATION_STATE state = Storage.getApplicationState(connection);
            if (state == null) {
                Storage.log("No previous state detected.");
                Storage.transitionToApplicationState(Storage.APPLICATION_STATE.INITIAL_LOAD_FROM_LIBRIS, connection);
            } else {
                Storage.log("Application starting in state: " + state);
            }
            connection.commit();
        }

        server.start();

        // The main loop
        while (true) {
            Connection connection = Storage.getConnection();
            Storage.APPLICATION_STATE state = Storage.getApplicationState(connection);

            switch (state) {
                case INITIAL_LOAD_FROM_LIBRIS:
                    EmmDumpImport.run();
                    break;
                case INITIAL_LOAD_TO_FOLIO:
                    // TEMP! ACTUALLY SYNC TO FOLIO EVENTUALLY!
                    Storage.transitionToApplicationState(Storage.APPLICATION_STATE.STAYING_IN_SYNC, connection);
                    connection.commit();
                    break;
                case STAYING_IN_SYNC: {
                    boolean changesMade = false;
                    changesMade |= EmmSync.run();
                    FolioSync.run();
                    if (!changesMade) {
                        Thread.sleep(100);
                    }
                    break;
                }
            }
        }

        //server.join(); // unreachable
    }
}
