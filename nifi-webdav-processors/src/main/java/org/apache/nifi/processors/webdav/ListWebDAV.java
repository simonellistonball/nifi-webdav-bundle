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
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.util.*;

@Tags({"webdav", "list"})
@CapabilityDescription("List Files in a WebDAV folders")
@WritesAttributes({@WritesAttribute(attribute = "filename", description = "Filename of resource"), @WritesAttribute(attribute = "path", description = "Path of resource"),
        @WritesAttribute(attribute = "etag", description = "Resource etag"),
        @WritesAttribute(attribute = "mime.type", description = "Content type of resource"),
        @WritesAttribute(attribute = "isDirectory", description = "Is content directory"),
        @WritesAttribute(attribute = "url", description = "URL to content"),
        @WritesAttribute(attribute = "date.created", description = "Date created (timestamp)"),
        @WritesAttribute(attribute = "date.modified", description = "Date modified (timestamp)")
})
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Stateful(scopes = {Scope.CLUSTER}, description = "After performing a listing of files, the timestamp of the newest file is stored. "
        + "This allows the Processor to list only files that have been added or modified after "
        + "this date the next time that the Processor is run. State is stored across the cluster so that this Processor can be run on Primary Node only and if "
        + "a new Primary Node is selected, the new node will not duplicate the data that was listed by the previous Primary Node.")
public class ListWebDAV extends AbstractWebDAVProcessor {

    public static final PropertyDescriptor DEPTH = new PropertyDescriptor.Builder().name("Search Depth").description("The depth of links to follow for new collections")
            .addValidator(StandardValidators.INTEGER_VALIDATOR).defaultValue("1").build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {

        properties = List.of(URL, DEPTH, SSL_CONTEXT_SERVICE, USERNAME, PASSWORD, NTLM_AUTH, PROXY_CONFIGURATION_SERVICE,
                PROXY_HOST, PROXY_PORT, HTTP_PROXY_USERNAME, HTTP_PROXY_PASSWORD, NTLM_PROXY_AUTH);

        relationships = Set.of(REL_SUCCESS);
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

        String url = context.getProperty(URL).evaluateAttributeExpressions().getValue();
        addAuth(context, url);

        long lastModified;
        try {
            lastModified = Long.parseLong(readState(context, "lastModified", "0"));
        } catch (IOException e) {
            throw new ProcessException("Failed read State", e);
        }

        LinkedList<FlowFile> files = new LinkedList<>();
        long maxModified;
        try {
            maxModified = listDAVFile(session, url, context.getProperty(DEPTH).asInteger(), files, lastModified);
        } catch (IOException e) {
            throw new ProcessException("Failed List Files", e);
        }


        if (!files.isEmpty()) {
            session.transfer(files, REL_SUCCESS);
            try {
                saveState(context, "lastModified", String.valueOf(maxModified));
            } catch (IOException e) {
                throw new ProcessException("Failed write state", e);
            }

        }

    }

    private long listDAVFile(ProcessSession session, String url, int depth, LinkedList<FlowFile> files, long lastModified) throws IOException {
        long maxModified = 0;
        Sardine sardine = buildSardine();
        List<DavResource> list;
        list = sardine.list(url, depth);
        for (final DavResource resource : list) {
            final long modifiedAt = resource.getModified().getTime();
            if (modifiedAt > lastModified) {
                files.add(createFile(session, resource));
                if (modifiedAt > maxModified)
                    maxModified = modifiedAt;
            }
        }
        return maxModified;
    }

    private FlowFile createFile(ProcessSession session, DavResource resource) {
        FlowFile flowFile = session.create();
        Map<String, String> attributes = createAttributes(resource);
        return session.putAllAttributes(flowFile, attributes);
    }

    private Map<String, String> createAttributes(DavResource resource) {
        return new HashMap<>() {
            private static final long serialVersionUID = 1L;

            {
                put("filename", resource.getName());
                put("path", resource.getPath());
                put("etag", resource.getEtag());
                put("mime.type", resource.getContentType());
                put("isDirecotry", String.valueOf(resource.isDirectory()));
                if (resource.getCreation() != null)
                    put("date.created", String.valueOf(resource.getCreation().getTime()));
                if (resource.getModified() != null)
                    put("date.modified", String.valueOf(resource.getModified().getTime()));
            }
        };
    }

    private String readState(ProcessContext context, String name, String defaultValue) throws IOException {
        StateManager stateManager = context.getStateManager();
        StateMap state = stateManager.getState(Scope.CLUSTER);
        if (state.get(name) != null) {
            return state.get("lastModified");
        } else return defaultValue;
    }

    private void saveState(ProcessContext context, String name, String value) throws IOException {
        Map<String, String> newState = new HashMap<>();
        newState.put(name, value);
        context.getStateManager().setState(newState, Scope.CLUSTER);
    }
}
