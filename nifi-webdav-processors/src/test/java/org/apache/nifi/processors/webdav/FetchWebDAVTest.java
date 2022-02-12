/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.webdav;

import org.apache.nifi.processors.webdav.MockSardine.MockDavResource;
import org.apache.nifi.processors.webdav.MockSardine.MockSardine;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.URISyntaxException;
import java.util.Date;


public class FetchWebDAVTest {

    private TestRunner testRunner;

    @Before
    public void init() throws URISyntaxException {
        // start a dummy webdav server
        MockSardine mockSardine = new MockSardine();
        FetchWebDAV test = Mockito.spy(new FetchWebDAV());
        Mockito.doReturn(mockSardine).when(test).buildSardine();
        mockSardine.addResource(new MockDavResource("/test/file1", new Date(1000), "mime/test", 4L));
        testRunner = TestRunners.newTestRunner(test);
    }

    @Test
    public void testProcessor() {
        testRunner.assertNotValid();
        testRunner.setProperty(FetchWebDAV.URL, "https://test.com/webdav/test/file");
        testRunner.setProperty(FetchWebDAV.USERNAME, "test");
        testRunner.setProperty(FetchWebDAV.PASSWORD, "test");
        testRunner.setProperty(FetchWebDAV.GET_ALL_PROPS, "true");
        testRunner.setValidateExpressionUsage(false);
        testRunner.assertValid();
        testRunner.enqueue(new MockFlowFile(0));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(ListWebDAV.REL_SUCCESS);
        testRunner.assertTransferCount(ListWebDAV.REL_SUCCESS, 1);
        testRunner.assertAllConditionsMet(ListWebDAV.REL_SUCCESS, mockFlowFile -> mockFlowFile.isContentEqual("test"));
    }

}
