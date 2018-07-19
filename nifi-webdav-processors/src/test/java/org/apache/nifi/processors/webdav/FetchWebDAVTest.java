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

import java.util.List;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class FetchWebDAVTest {

    private TestRunner testRunner;

    // The url of the test file in the WebDAV
    private static final String WEBDAV_TARGET_URL =  "";

    @Before
    public void init() {
        // start a dummy webdav server
        testRunner = TestRunners.newTestRunner(FetchWebDAV.class);
        testRunner.setValidateExpressionUsage(false);

    }

    @Test
    public void testProcessor() throws Exception {

        testRunner.setProperty(FetchWebDAV.URL, WEBDAV_TARGET_URL);

        testRunner.enqueue("");

        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(FetchWebDAV.RELATIONSHIP_SUCCESS);

        List<MockFlowFile> successFiles = testRunner.getFlowFilesForRelationship(FetchWebDAV.RELATIONSHIP_SUCCESS);
        Assert.assertEquals(1, successFiles.size());
        MockFlowFile successOne = successFiles.get(0);
        System.out.println("File attributes : " + successOne.getAttributes());
        System.out.println("File content : " + new String(testRunner.getContentAsByteArray(successOne), "UTF-8"));
    }
}
