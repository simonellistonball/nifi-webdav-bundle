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

import com.github.sardine.DavResource;
import com.github.sardine.Sardine;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

@Tags({"webdav", "fetch"})
@CapabilityDescription("Fetches content from a WebDAV resource")
@SeeAlso({ListWebDAV.class})
@ReadsAttributes({@ReadsAttribute(attribute = "filename", description = "Filename of resource"), @ReadsAttribute(attribute = "path", description = "Path of resource")})
@InputRequirement(Requirement.INPUT_REQUIRED)
public class FetchWebDAV extends AbstractWebDAVProcessor {

    private static final PropertyDescriptor GET_ALL_PROPS = new PropertyDescriptor.Builder()
            .name("Get All Properties")
            .description("Whether to fetch all properties for the resource")
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues(TRUE_VALUE,FALSE_VALUE)
            .build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {

        properties = List.of(URL, GET_ALL_PROPS, SSL_CONTEXT_SERVICE, USERNAME, PASSWORD, NTLM_AUTH,
                PROXY_CONFIGURATION_SERVICE, PROXY_HOST, PROXY_PORT, HTTP_PROXY_USERNAME, HTTP_PROXY_PASSWORD,
                NTLM_PROXY_AUTH);

        relationships = Set.of(REL_SUCCESS, REL_FAILURE);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        boolean getAllProperties = context.getProperty(GET_ALL_PROPS).evaluateAttributeExpressions(flowFile).asBoolean();
        try {
            try {
                String url = context.getProperty(URL).evaluateAttributeExpressions(flowFile).getValue();
                addAuth(context, url);
                Sardine sardine = buildSardine();
                // get all the properties
                if (getAllProperties) {
                    flowFile = session.putAllAttributes(flowFile, getProperties(sardine,url));
                }
                flowFile = session.importFrom(sardine.get(url), flowFile);
                session.transfer(flowFile, REL_SUCCESS);
            } catch (Exception e1) {
                flowFile = session.penalize(flowFile);
                session.transfer(flowFile, REL_FAILURE);
                getLogger().error("Error processing FlowFile", e1);
            }
        } catch (Exception e) {
            context.yield();
            throw new ProcessException("Error building Sardine client",e);

        }
    }

    private Map<String,String> getProperties(Sardine sardine,String url) throws IOException {
        DavResource resource = sardine.list(url, 0, true).get(0);
        Map<String, String> customProps = resource.getCustomProps();
        Map<String, String> attributes = new HashMap<>(customProps.size());
        for (Entry<String, String> entry : customProps.entrySet()) {
            attributes.put("dav." + entry.getKey(), entry.getValue());
        }
        return attributes;
    }
}
