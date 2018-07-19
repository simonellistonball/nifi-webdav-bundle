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
import java.util.*;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import com.github.sardine.Sardine;
import org.apache.nifi.processor.util.StandardValidators;

@Tags({ "webdav", "delete" })
@CapabilityDescription("Deletes WebDAV resource")
@SeeAlso({ ListWebDAV.class })
public class DeleteWebDAV extends AbstractWebDAVProcessor {

    public static final PropertyDescriptor DELETE_NON_EMPTY_DIRECTORY =
            new PropertyDescriptor.Builder()
                    .name("Delete non-empty directory")
                    .description("Whether to delete a directory that is not empty.")
                    .required(false)
                    .defaultValue("false")
                    .allowableValues("true", "false")
                    .addValidator(StandardValidators.BOOLEAN_VALIDATOR).expressionLanguageSupported(true).build();

    public static final Relationship RELATIONSHIP_NO_ACTTION = new Relationship.Builder().name("no-action").description("No action was taken").build();

    private final static List<PropertyDescriptor> properties;
    private final static Set<Relationship> relationships;

    static {
        final List<PropertyDescriptor> _properties = new ArrayList<>();
        _properties.add(URL);
        _properties.add(DELETE_NON_EMPTY_DIRECTORY);

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
        _relationships.add(RELATIONSHIP_NO_ACTTION);
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

        boolean deleteIfNonEmpty = context.getProperty(DELETE_NON_EMPTY_DIRECTORY).evaluateAttributeExpressions(flowFile).asBoolean();

        try {
            String url = context.getProperty(URL).evaluateAttributeExpressions(flowFile).getValue();

            addAuth(context, url);
            final Sardine sardine = buildSardine(context);

            if(!deleteIfNonEmpty && sardine.list(url, 1).size() > 1) {
                session.transfer(flowFile, RELATIONSHIP_NO_ACTTION);
            }
            else {
                sardine.delete(url);
                session.transfer(flowFile, RELATIONSHIP_SUCCESS);
            }

        } catch (IOException e) {
            getLogger().error("Failed to delete WebDAV resource", e);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, RELATIONSHIP_FAILURE);
        }
    }
}
