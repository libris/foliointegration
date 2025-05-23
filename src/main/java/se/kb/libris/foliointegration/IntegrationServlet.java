package se.kb.libris.foliointegration;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class IntegrationServlet extends HttpServlet {
    protected void doGet(HttpServletRequest request, HttpServletResponse response) {
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
