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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NetflowParser {

    protected static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public class NetflowField {
        public NetflowField(int type, int len, int en) {
            this.type = type;
            this.length = len;
            this.en = en;
        }

        public int type;
        public int length;
        public int en;

        public String getName() {
            if (this.en != 0) {
                return IANA_IPFIX.fromCode(type).name() + "_" + String.valueOf(this.en);
            }
            return IANA_IPFIX.fromCode(type).name();
        }

        @Override
        public String toString() {
            return String.format("Field [name=%s, type=%d, en=%d, length=%d]",
                    new Object[] { getName(), type, en, length });
        }

        public String convertToString(byte[] dst) {
            IANA_IPFIX iana = IANA_IPFIX.fromCode(type);
            return iana.getType().convertToString(dst, length);
        }
    }

    public class NetflowTemplate {
        public NetflowTemplateKey key;
        public List<NetflowField> fields = new ArrayList<>();

        public NetflowTemplate(NetflowTemplateKey key) {
            this.key = key;
        }

        public void add(NetflowField field) {
            this.fields.add(field);
        }

        @Override
        public String toString() {
            return String.format("NetflowTemplate [key=%s,fields=%d]", new Object[] { key.id, fields.size() });
        }
    }

    public class NetflowTemplateKey {
        public NetflowTemplateKey(int sourceId, int domainId, int templateId) {
            this.sourceId = sourceId;
            this.domainId = domainId;
            this.id = templateId;
        }

        /**
         * The device sending the flow
         */
        public int sourceId;
        /**
         * Observation domain id in IPFIX protocol
         */
        public int domainId;
        /**
         * The template identifier
         */
        public int id;

        @Override
        public String toString() {
            return String.format("NetflowTemplateKey [sourceId=%x, domainId=%x, id=%x]",
                    new Object[] { sourceId, domainId, id });
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            // result = prime * result + getOuterType().hashCode();
            result = prime * result + id;
            result = prime * result + domainId;
            result = prime * result + sourceId;
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            NetflowTemplateKey other = (NetflowTemplateKey) obj;
            if (!getOuterType().equals(other.getOuterType()))
                return false;
            if (id != other.id)
                return false;
            if (domainId != other.domainId)
                return false;
            if (sourceId != other.sourceId)
                return false;
            return true;
        }

        private NetflowParser getOuterType() {
            return NetflowParser.this;
        }
    }

    public class NetflowRecord extends HashMap<String, String> implements Map<String, String> {
        private static final long serialVersionUID = 1L;
        private NetflowTemplate template;

        public NetflowTemplate getTemplate() {
            return template;
        }

        public void setTemplate(NetflowTemplate template) {
            this.template = template;
        }
    }

    private DataInputStream ins;
    private ConcurrentHashMap<NetflowTemplateKey, NetflowTemplate> templates = new ConcurrentHashMap<NetflowTemplateKey, NetflowTemplate>();
    private ConcurrentLinkedQueue<NetflowRecord> records = new ConcurrentLinkedQueue<NetflowRecord>();
    private ConcurrentLinkedQueue<byte[]> readyToReRun = new ConcurrentLinkedQueue<byte[]>();
    private ConcurrentHashMap<NetflowTemplateKey, Queue<byte[]>> heldSets = new ConcurrentHashMap<NetflowTemplateKey, Queue<byte[]>>();

    // private int fileLength;
    private int exportTime;
    private int sequenceNumber;
    private int domainId;
    private int sourceId;

    public NetflowParser() {
        super();
    }

    public void setStream(DataInputStream s) throws IOException {
        if (this.ins != null) {
            this.ins.close();
        }
        this.ins = s;
    }

    private void parseStream(DataInputStream s) throws IOException {
        while (s.available() > 4) {
            int fileLength = processHeader(s);
            // check this offset calculation to ensure if matches the right header overhead
            int processed = 16;
            while (processed < fileLength) {
                processed += processSet(s);
                logger.debug(String.format("Finished set, processed = %d of fileLength = %d",
                        new Object[] { processed, fileLength }));
            }
            logger.debug(String.format("Templates: %d, Records: %d in %d bytes",
                    new Object[] { this.templates.size(), this.records.size(), processed }));
        }
    }

    public void parse() throws IOException {
        if (ins == null) {
            throw new IOException("No stream attached to parser");
        } else {
            parseStream(ins);
        }
        // process any packets which have been delayed, but can now be processed because
        // their templates are available.
        rerun();
    }

    private void rerun() throws IOException {
        byte[] replay;
        replay = readyToReRun.poll();
        while (replay != null) {
            DataInputStream s = new DataInputStream(new ByteArrayInputStream(replay));
            parseStream(s);
            replay = readyToReRun.poll();
        }
    }

    private int processHeader(DataInputStream s) throws IOException {
        int version = s.readUnsignedShort();
        if (version != 10) {
            throw new IOException("Invalid file version");
        }
        int fileLength = s.readUnsignedShort();
        exportTime = s.readInt();
        sequenceNumber = s.readInt();
        domainId = s.readInt();

        logger.debug(String.format("Message Headers %d bytes at %d sequence=%d, domain=%d",
                new Object[] { fileLength, exportTime, sequenceNumber, domainId }));
        return fileLength;
    }

    /**
     * @return number of bytes processed
     */
    private int processSet(DataInputStream s) throws IOException {
        int setId = s.readUnsignedShort();
        int length = s.readUnsignedShort();

        int bytes = length;

        // what type of record are we dealing with?
        if (setId == 2) {
            // regular template
            bytes = processTemplate(s, length, false);
        } else if (setId == 3) {
            // option template
            bytes = processTemplate(s, length, true);
        } else if (setId > 255) {
            // data packet
            NetflowTemplateKey key = new NetflowTemplateKey(sourceId, domainId, setId);
            NetflowTemplate template = templates.get(key);
            if (template == null) {
                logger.error(String.format("Missing template: %s", new Object[] { key }));
                // Save the set for later by constructing an IPFIX packet for replay
                byte[] replayBytes = new byte[length + 16 + 4];
                ByteBuffer bb = ByteBuffer.wrap(replayBytes);
                bb.put((byte) 0x00);
                bb.put((byte) 0x0a);
                bb.putShort((short) (length + 16 + 4));
                bb.putInt(exportTime);
                bb.putInt(sequenceNumber);
                bb.putInt(domainId);

                bb.putShort((short) setId);
                bb.putShort((short) length);

                byte[] setBytes = new byte[length];
                s.read(setBytes, 0, length);
                bb.put(setBytes);

                // store the packet to re-process based on arrival of a template for the given
                // domain and template id
                if (!heldSets.containsKey(key)) {
                    heldSets.put(key, new ConcurrentLinkedQueue<byte[]>());
                }
                heldSets.get(key).add(bb.array());
                logger.debug(String.format("Holding set for template %x (%d bytes)",
                        new Object[] { setId, bb.array().length }));
            } else {
                logger.debug(String.format("Using template %s", new Object[] { template.toString() }));
                bytes = processDataRecord(s, template, length);
            }
        } else {
            // strange packet, log content
            byte[] b = new byte[length];
            s.read(b, 0, length);
            logger.error(String.format("Unexpected reserved set id %d for %d bytes [%s]",
                    new Object[] { setId, length, Hex.encodeHexString(b) }));
        }
        logger.debug(String.format("Processed Set %x of %d bytes using %d", new Object[] { setId, length, bytes }));
        if (bytes < length) {
            s.skip(length + 4 - bytes);
        }
        return length + 4;
    }

    private int processTemplate(DataInputStream s, int length, boolean option) throws IOException {
        int bytesProcessed = 4;
        while (bytesProcessed < length) {
            int templateId = s.readUnsignedShort();
            int fieldCount = s.readUnsignedShort();
            bytesProcessed += 4;
            int scopeFieldCount = 0;
            if (option) {
                scopeFieldCount = s.readUnsignedShort();
                bytesProcessed += 2;
            }
            NetflowTemplateKey key = new NetflowTemplateKey(sourceId, domainId, templateId);
            if (fieldCount == 0 && this.templates.containsKey(key)) {
                // template withdrawn
                this.templates.remove(key);
            } else {
                NetflowTemplate template = new NetflowTemplate(key);
                for (int i = 0, l = fieldCount; i < l; i++) {
                    NetflowField field = processField(s);
                    template.add(field);
                    bytesProcessed += 4;
                    if (field.en != 0) {
                        bytesProcessed += 4;
                    }
                }

                logger.debug(String.format(
                        "Processed Template %x with %d fields and %d scopeFields (bytesProcessed=%d, length=%d)",
                        new Object[] { key.id, fieldCount, scopeFieldCount, bytesProcessed, length }));
                if (logger.isDebugEnabled()) {
                    for (NetflowField field : template.fields) {
                        logger.debug(field.toString());
                    }
                }
                this.templates.put(key, template);
                // check for any saved packets that can now be reprocessed
                logger.debug(
                        String.format("Checking heldsets (%d) key: %s", new Object[] { this.heldSets.size(), key }));

                if (this.heldSets.containsKey(key)) {
                    logger.debug(String.format("Re-running held datasets for template %s", new Object[] { key }));
                    this.readyToReRun.addAll(this.heldSets.get(key));
                    this.heldSets.remove(key);
                    rerun();
                }
            }
        }
        return bytesProcessed;
    }

    private NetflowField processField(DataInputStream s) throws IOException {
        int type = s.readUnsignedShort();
        int len = s.readUnsignedShort();
        int en = 0;
        if ((type & 0x8000) == 0x8000) {
            en = s.readInt();
        }
        return new NetflowField((type & 0x7FFF), len, en);
    }

    private int processDataRecord(DataInputStream s, NetflowTemplate template, int length) throws IOException {
        int bytesProcessed = 4;
        while (bytesProcessed < length) {
            NetflowRecord record = new NetflowRecord();
            record.setTemplate(template);
            for (NetflowField item : template.fields) {
                int fieldLength = item.length;
                if (fieldLength == 65535) {
                    // this is a variable length field, which means the first byte is the length of
                    // the value
                    fieldLength = s.read();
                    if (fieldLength == 255) {
                        fieldLength = s.readUnsignedShort();
                        bytesProcessed += 2;
                    }
                    bytesProcessed += 1;
                    logger.debug(String.format("Custom length found %d of type %d",
                            new Object[] { fieldLength, item.type }));
                }
                byte[] dst = new byte[fieldLength];
                s.read(dst, 0, fieldLength);
                bytesProcessed += fieldLength;
                record.put(item.getName(), item.convertToString(dst));
            }
            this.records.add(record);
            logger.debug(String.format("Flow Record (template=%x, length=%d, bytes=%d)",
                    new Object[] { template.key.id, length, bytesProcessed }));
            logger.debug(record.toString());
        }
        return bytesProcessed;
    }

    public Map<NetflowTemplateKey, NetflowTemplate> getTemplates() {
        return templates;
    }

    public Queue<NetflowRecord> getRecords() {
        return records;
    }

    /**
     * Build a list of all known fields in the templates
     *
     * @return Set of all the field identifiers across all templates
     */
    public Set<Integer> getAllKnownFields() {
        Set<Integer> fieldTypes = this.templates.values().stream().flatMap(x -> x.fields.stream().map(f -> f.type))
                .collect(Collectors.toSet());
        return fieldTypes;
    }

    public void close() throws IOException {
        this.ins.close();
    }

    public Set<String> getAllKnownFieldsTypes() {
        // get every field, and convert to name
        return this.templates.values().stream().map(t -> t.fields).flatMap(List::stream).map(f -> f.getName())
                .collect(Collectors.toSet());
    }

    public void setSourceId(int sourceId) {
        this.sourceId = sourceId;
    }

}
