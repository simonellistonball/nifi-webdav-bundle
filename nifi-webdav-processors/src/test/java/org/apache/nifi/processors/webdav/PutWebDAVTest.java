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

import org.apache.nifi.processors.webdav.MockSardine.MockSardine;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;


public class PutWebDAVTest {

    private TestRunner testRunner;
    private MockSardine mockSardine;

    @Before
    public void init() {
        // start a dummy webdav server
        mockSardine = new MockSardine();
        PutWebDAV test = Mockito.spy(new PutWebDAV());
        Mockito.doReturn(mockSardine).when(test).buildSardine();
        testRunner = TestRunners.newTestRunner(test);
    }

    @Test
    public void testProcessor() {
        testRunner.assertNotValid();
        testRunner.setProperty(FetchWebDAV.URL, "https://test.com/webdav/test/file");
        testRunner.setProperty(FetchWebDAV.USERNAME, "test");
        testRunner.setProperty(FetchWebDAV.PASSWORD, "test");
        testRunner.setValidateExpressionUsage(false);
        testRunner.assertValid();
        Map<String, String> attrs = new HashMap<>();
        attrs.put("mime.test", "type/test");
        testRunner.enqueue("test", attrs);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(ListWebDAV.REL_SUCCESS);
        testRunner.assertTransferCount(ListWebDAV.REL_SUCCESS, 1);
        testRunner.assertAllConditionsMet(ListWebDAV.REL_SUCCESS, mockFlowFile -> mockFlowFile.isContentEqual("test"));
        assert this.mockSardine.putCount==1;
    }

}
