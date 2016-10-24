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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import com.github.sardine.DavResource;
import com.github.sardine.Sardine;

@Tags({ "webdav", "list" })
@CapabilityDescription("List Files in a WebDAV folder")
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "filename", description = "Filename of resource"), @WritesAttribute(attribute = "path", description = "Path of resource"),
        @WritesAttribute(attribute = "etag", description = "Resource etag"), @WritesAttribute(attribute = "mime.type", description = "Content type of resource"),
        @WritesAttribute(attribute = "date.created", description = "Date created (timestamp)"), @WritesAttribute(attribute = "date.modified", description = "Date modified (timestamp)") })
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Stateful(scopes = { Scope.CLUSTER }, description = "After performing a listing of files, the timestamp of the newest file is stored. "
        + "This allows the Processor to list only files that have been added or modified after "
        + "this date the next time that the Processor is run. State is stored across the cluster so that this Processor can be run on Primary Node only and if "
        + "a new Primary Node is selected, the new node will not duplicate the data that was listed by the previous Primary Node.")
public class ListWebDAV extends AbstractWebDAVProcessor {

    public static final PropertyDescriptor DEPTH = new PropertyDescriptor.Builder().name("Search Depth").description("The depth of links to follow for new collections")
            .addValidator(StandardValidators.INTEGER_VALIDATOR).defaultValue("1").build();

    private final static List<PropertyDescriptor> properties;
    private final static Set<Relationship> relationships;

    static {
        final List<PropertyDescriptor> _properties = new ArrayList<>();
        _properties.add(URL);
        _properties.add(DEPTH);

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
        relationships = Collections.unmodifiableSet(_relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        StateManager stateManager = context.getStateManager();

        String url = context.getProperty(URL).evaluateAttributeExpressions().getValue();
        addAuth(context, url);
        Sardine sardine = buildSardine(context);
        String lastModifiedState = null;
        try {
            StateMap state = stateManager.getState(Scope.CLUSTER);
            lastModifiedState = state.get("lastModified");
        } catch (IOException e) {
            getLogger().error("Failed to load state", e);
        }
        
        if (lastModifiedState == null) lastModifiedState = "0";
        long lastModified = Long.valueOf(lastModifiedState);
        long maxModified = 0;
        
        int depth = context.getProperty(DEPTH).asInteger();

        List<DavResource> list;
        LinkedList<FlowFile> files = new LinkedList<FlowFile>();
        try {
            list = sardine.list(url, depth);

            for (final DavResource resource : list) {
                final long modifiedAt = resource.getModified().getTime();
                if (modifiedAt <= lastModified) {
                    continue;
                } else {
                    final long createdAt = resource.getCreation().getTime();

                    FlowFile flowFile = session.create();
                    Map<String, String> attributes = new HashMap<String, String>() {
                        private static final long serialVersionUID = 1L;

                        {
                            put("url", resource.getName());
                            put("path", resource.getPath());
                            put("etag", resource.getEtag());
                            put("mime.type", resource.getContentType());
                            put("date.created", String.valueOf(createdAt));
                            put("date.modified", String.valueOf(modifiedAt));
                        }
                    };
                    flowFile = session.putAllAttributes(flowFile, attributes);
                    files.add(flowFile);
                    // store the modified dates in Processor State to avoid duplication
                    if (modifiedAt > maxModified)
                        maxModified = modifiedAt;
                }
            }
        } catch (IOException e) {
            context.yield();
            throw new ProcessException("Failed to list webdav resources", e);
        }

        if (!files.isEmpty()) {
            session.transfer(files, RELATIONSHIP_SUCCESS);
            Map<String, String> newState = new HashMap<String, String>();
            newState.put("lastModified", String.valueOf(maxModified));
            try {
                stateManager.setState(newState, Scope.CLUSTER);
            } catch (IOException e) {
                getLogger().error("Failed to save state", e);
            }
        }

    }
}
