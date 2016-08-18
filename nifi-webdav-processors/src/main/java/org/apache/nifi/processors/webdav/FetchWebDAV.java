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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import com.github.sardine.DavResource;
import com.github.sardine.Sardine;

@Tags({ "webdav", "fetch" })
@CapabilityDescription("Fetches content from a WebDAV resource")
@SeeAlso({ ListWebDAV.class })
@ReadsAttributes({ @ReadsAttribute(attribute = "filename", description = "Filename of resource"), @ReadsAttribute(attribute = "path", description = "Path of resource") })
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })
@InputRequirement(Requirement.INPUT_REQUIRED)
public class FetchWebDAV extends AbstractWebDAVProcessor {

    private static final PropertyDescriptor GET_ALL_PROPS = new PropertyDescriptor.Builder().name("Get All Properties").description("Whether to fetch all properties for the resource").required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR).expressionLanguageSupported(true).build();

    private final static List<PropertyDescriptor> properties;
    private final static Set<Relationship> relationships;

    static {
        final List<PropertyDescriptor> _properties = new ArrayList<>();
        _properties.add(URL);
        _properties.add(GET_ALL_PROPS);

        _properties.add(SSL_CONTEXT_SERVICE);
        _properties.add(USERNAME);
        _properties.add(PASSWORD);
        _properties.add(NTLM_AUTH);
        
        _properties.add(PROXY_HOST);
        _properties.add(PROXY_PORT);
        _properties.add(HTTP_PROXY_USERNAME);
        _properties.add(HTTP_PROXY_PASSWORD);
        _properties.add(NTLM_PROXY_AUTH);
        properties = Collections.unmodifiableList(_properties);

        final Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(RELATIONSHIP_SUCCESS);
        _relationships.add(RELATIONSHIP_FAILURE);
        relationships = Collections.unmodifiableSet(_relationships);
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
                String url = flowFile.getAttribute("path");
                addAuth(context, url);

                Sardine sardine = buildSardine(context);

                // get all the properties
                if (getAllProperties) {
                    DavResource resource = sardine.list(url, 0, true).get(0);
                    Map<String, String> customProps = resource.getCustomProps();
                    Map<String, String> attributes = new HashMap<String, String>(customProps.size());
                    for (Entry<String, String> entry : customProps.entrySet()) {
                        attributes.put("dav." + entry.getKey(), entry.getValue());
                    }
                    flowFile = session.putAllAttributes(flowFile, attributes);
                }
                flowFile = session.importFrom(sardine.get(url), flowFile);
                session.transfer(flowFile, RELATIONSHIP_SUCCESS);
            } catch (Exception e1) {
                getLogger().error("Error processing FlowFile", e1);
                flowFile = session.penalize(flowFile);
                session.transfer(flowFile, RELATIONSHIP_FAILURE);
            }
        } catch (Exception e) {
            getLogger().error("Error building Sardine client", e);
            context.yield();
        }
    }
}
