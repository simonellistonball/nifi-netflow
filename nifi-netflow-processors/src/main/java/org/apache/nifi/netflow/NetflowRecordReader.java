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

import java.io.EOFException;
import java.io.IOException;
import java.util.Collections;
import java.util.Queue;
import java.util.stream.Collectors;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.netflow.NetflowParser.NetflowRecord;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

/**
 * Assumes that the inbound flow file contains a single netflow packet, which
 * may represent a large number of records. This implementation processes as
 * entire netflow packet as a single unit, and as such is not a reusable reader.
 *
 * @author sball
 */
public class NetflowRecordReader implements RecordReader {
    private final ComponentLog logger;
    private final NetflowParser parser;

    public NetflowRecordReader(ComponentLog logger, NetflowParser parser) {
        this.logger = logger;
        this.parser = parser;
    }

    @Override
    public void close() throws IOException {
        this.parser.close();
    }

    @Override
    public Record nextRecord(boolean coerceTypes, boolean dropUnknownFields)
            throws IOException, MalformedRecordException {

        Queue<NetflowRecord> records = this.parser.getRecords();
        // if the queue is empty, parse another packet
        if (records.isEmpty()) {
            try {
                this.parser.parse();
            } catch (EOFException e) {
                logger.error("Ran past stream", e);
            }
            records = this.parser.getRecords();
        }
        // look at the queue to see if we have records yet
        if (!records.isEmpty()) {
            return createRecord(records.poll());
        }
        // there may still be no records, particularly if we received a template only
        // packet
        return null;
    }

    private Record createRecord(NetflowRecord netflowRecord) {
        RecordSchema schema = new SimpleRecordSchema(netflowRecord.getTemplate().fields.stream()
                .map(f -> new RecordField(f.getName(), RecordFieldType.STRING.getDataType()))
                .collect(Collectors.toList()));
        return new MapRecord(schema, Collections.<String, Object>unmodifiableMap(netflowRecord));
    }

    @Override
    public RecordSchema getSchema() throws MalformedRecordException {
        RecordSchema schema = new SimpleRecordSchema(this.parser.getAllKnownFieldsTypes().stream()
                .map(f -> new RecordField(f, RecordFieldType.STRING.getDataType())).collect(Collectors.toList()));
        return schema;
    }

}
