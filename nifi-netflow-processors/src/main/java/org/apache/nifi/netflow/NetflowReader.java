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
package org.apache.nifi.netflow;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.RecordSchema;

@Tags({ "netflow", "networking", "listen", "metron", "record", "ipfix" })
@CapabilityDescription("Reads packets in Cisco Netflow 5, 9 and IPFIX formats and outputs a Record representation")
public class NetflowReader extends AbstractControllerService implements RecordReaderFactory {

    static final AllowableValue NETFLOW_5 = new AllowableValue("netflow-5", "Netflow 5", "Netflow v5");
    static final AllowableValue NETFLOW_9 = new AllowableValue("netflow-9", "Netflow 9", "Netflow v9");
    static final AllowableValue IPFIX = new AllowableValue("ipfix", "IPFIX", "IPFIX");
    static final AllowableValue NETFLOW_ANY = new AllowableValue("any", "Any",
            "Parse any format we receive based on the version in the header");

    static final PropertyDescriptor NETFLOW_VERSION = new PropertyDescriptor.Builder().name("netflow-version")
            .displayName("Netflow Version").description("Which version of netflow should the reader parse?")
            .allowableValues(NETFLOW_5, NETFLOW_9, IPFIX, NETFLOW_ANY).defaultValue(NETFLOW_ANY.getValue())
            .required(true).build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(NETFLOW_VERSION);
        return properties;
    }

    @Override
    public RecordReader createRecordReader(Map<String, String> flowFile, InputStream in, ComponentLog logger)
            throws MalformedRecordException, IOException, SchemaNotFoundException {
        final RecordSchema schema = null;
        return new NetflowRecordReader(in, schema, logger);
    }

}
