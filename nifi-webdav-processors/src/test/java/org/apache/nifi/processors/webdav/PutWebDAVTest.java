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

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class PutWebDAVTest {

    private TestRunner testRunner;

    // The base url of the WebDAV
    private static final String WEBDAV_BASE_URL =  "";

    @Before
    public void init() {
        // start a dummy webdav server
        testRunner = TestRunners.newTestRunner(PutWebDAV.class);
    }

    @Test
    public void testProcessor() {

        Map<String, String> attributes = new HashMap<>();
        attributes.put("filename", "test2.txt");
        attributes.put("path", "superDir/subDir");

        testRunner.setProperty(PutWebDAV.URL, WEBDAV_BASE_URL + "/${path}/${filename}");
        testRunner.enqueue("This is a test file.", attributes);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutWebDAV.RELATIONSHIP_SUCCESS);
    }

}
