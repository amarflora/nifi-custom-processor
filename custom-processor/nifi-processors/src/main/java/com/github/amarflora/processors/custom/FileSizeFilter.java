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
package com.github.amarflora.processors.custom;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.*;

@Tags({"example"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class FileSizeFilter extends AbstractProcessor {

    private static int MAX_FILE_SIZE=0;

    public static final PropertyDescriptor MAX_FILE_SIZE_PROPERTY = new PropertyDescriptor.Builder()
            .name("Maximum File Size")
            .description("The maximum size that a file can be in order to be pulled")
            .required(true)
            .defaultValue("Enter max file size here")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship SUCCESS_RELATIONSHIP = new Relationship.Builder()
            .name("success")
            .description("All files are routed to success")
            .build();

    public static final Relationship FAIL_RELATIONSHIP = new Relationship.Builder()
            .name("failure")
            .description("Failed files will be routed to failure")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(MAX_FILE_SIZE_PROPERTY);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS_RELATIONSHIP);
        relationships.add(FAIL_RELATIONSHIP);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }

        // TODO implement

        String attributeMax=context.getProperty(MAX_FILE_SIZE_PROPERTY).getValue();
        getLogger().info("Got a flowfile");

        if (flowFile.getAttribute(attributeMax)!=null)
        {
            MAX_FILE_SIZE=Integer.parseInt(flowFile.getAttribute(attributeMax));
            getLogger().info("Reset MAX file size value to "+MAX_FILE_SIZE+"   Attribute Ref:"+attributeMax);
            session.remove(flowFile);
            return;
        }

        if (flowFile.getSize()<MAX_FILE_SIZE)
        {
            session.transfer(flowFile,SUCCESS_RELATIONSHIP);
            getLogger().info("File passed, size is less than "+MAX_FILE_SIZE+" ( "+flowFile.getSize()+" )");
        }
        else
        {
            session.transfer(flowFile,FAIL_RELATIONSHIP);
            getLogger().info("File NOT passed, size is more than "+MAX_FILE_SIZE+" ( "+flowFile.getSize()+" )");
        }

    }
}
