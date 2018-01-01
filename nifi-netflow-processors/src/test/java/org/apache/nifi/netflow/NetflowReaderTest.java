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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.MockComponentLog;
import org.junit.Test;

public class NetflowReaderTest {

    static {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.netflow", "debug");

    }
    final ComponentLog logger = new MockComponentLog("client", NetflowParserTest.class);

    @Test
    public void testNetflowRecordReader() throws IOException, MalformedRecordException {
        final InputStream fis = new FileInputStream(new File("src/test/resources/example.ipfix"));
        RecordSchema schema = new SimpleRecordSchema(new ArrayList<RecordField>());
        NetflowRecordReader reader = createReader(fis, schema);

        final Record record = reader.nextRecord();
        assertNotNull(record);
        assertEquals("62.75.195.236", record.getAsString("destinationIPv4Address"));
    }

    private NetflowRecordReader createReader(InputStream is, RecordSchema schema) throws IOException {
        NetflowParser parser = new NetflowParser();
        parser.setStream(new DataInputStream(is));
        return new NetflowRecordReader(logger, parser);
    }

}