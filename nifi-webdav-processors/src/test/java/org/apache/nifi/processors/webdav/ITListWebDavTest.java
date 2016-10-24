package org.apache.nifi.processors.webdav;

import java.util.List;

import org.apache.catalina.servlets.DefaultServlet;
import org.apache.catalina.servlets.WebdavServlet;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletHandler;
import org.junit.Before;
import org.junit.Test;

public class ITListWebDavTest {

    private static final String TEST_URL = "http://test.cyberduck.ch/dav/basic/";
    private static final String TEST_USER = "jenkins";
    private static final String TEST_PASS = "jenkins";

    @Test
    public void testListWebDav() {
        final TestRunner runner = TestRunners.newTestRunner(new ListWebDAV());
        runner.setProperty(ListWebDAV.URL, TEST_URL);
        runner.setProperty(ListWebDAV.USERNAME, TEST_USER);
        runner.setProperty(ListWebDAV.PASSWORD, TEST_PASS);

        runner.assertValid();

        runner.run();

        runner.assertAllFlowFilesTransferred(ListWebDAV.RELATIONSHIP_SUCCESS);
        List<MockFlowFile> flowFilesForSuccess = runner.getFlowFilesForRelationship(ListWebDAV.RELATIONSHIP_SUCCESS);

        for (MockFlowFile f : flowFilesForSuccess) {
            f.assertAttributeExists("path");
        }
    }

    @Before
    public void createServlet() throws Exception {
        Server server = new Server(8080);
        ServletHandler handler = new ServletHandler();
        server.setHandler(handler);
        handler.addServletWithMapping(DefaultServlet.class, "/");
        handler.addServletWithMapping(WebdavServlet.class, "/webdav/");
        server.start();
        server.join();
    }
}
