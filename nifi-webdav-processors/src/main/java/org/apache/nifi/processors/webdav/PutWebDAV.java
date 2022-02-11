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

import com.github.sardine.Sardine;
import org.apache.http.protocol.HTTP;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Tags({"webdav", "egress"})
@CapabilityDescription("Pit Resourcse to a WebDAV location")
@SeeAlso({ListWebDAV.class, FetchWebDAV.class})
@ReadsAttributes({@ReadsAttribute(attribute = "mime.type", description = "The content type of the file")})
@DynamicProperty(name = "Custom Property", value = "Attribute Expression Language", expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES, description = "Addeds custom properties to the WebDAV resource")
public class PutWebDAV extends AbstractWebDAVProcessor {

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String url = context.getProperty(URL).evaluateAttributeExpressions(flowFile).getValue();
        addAuth(context, url);
        final Sardine sardine = buildSardine();

        final String contentType = flowFile.getAttribute("mime.type");
        final long contentLength = flowFile.getSize();

        final Map<String, String> headers = new HashMap<>() {
            private static final long serialVersionUID = 1L;

            {
                put(HTTP.CONTENT_TYPE, contentType);
                put(HTTP.CONTENT_LEN, String.valueOf(contentLength));
            }
        };

        try {
            session.read(flowFile, in -> {
                try {
                    sardine.put(url, in, headers);
                } catch (IOException e) {
                    getLogger().error("Failed to put file", e);
                    throw e;
                }
            });
            session.transfer(flowFile, REL_SUCCESS);
        } catch (Exception e) {
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            // TODO - update the properties on the resource if required and include dynamic properties
            // TODO - handle missing collections
        }
    }
}