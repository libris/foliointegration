package se.kb.libris.foliointegration;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class IntegrationServlet extends HttpServlet {
    protected void doGet(HttpServletRequest request, HttpServletResponse response) {
        response.setStatus(200);
    }

    protected void doPost(HttpServletRequest request, HttpServletResponse response) {
        response.setStatus(200);
    }
}
