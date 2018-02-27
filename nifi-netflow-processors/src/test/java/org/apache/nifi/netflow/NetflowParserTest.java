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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Queue;
import java.util.Set;

import org.apache.commons.codec.binary.Hex;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NetflowParserTest {

    protected static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Test
    public void testParser() throws IOException {
        final InputStream fis = new FileInputStream(new File("src/test/resources/example.ipfix"));
        final NetflowParser parser = new NetflowParser();
        parser.setStream(new DataInputStream(fis));
        parser.parse();
        assertEquals(10, parser.getTemplates().size());
        assertNotNull(parser.getRecords());
        assertEquals(35, parser.getRecords().size());
    }

    @Test
    public void testTheUniqueFieldBuilder() throws IOException {
        final InputStream fis = new FileInputStream(new File("src/test/resources/example.ipfix"));
        final NetflowParser parser = new NetflowParser();
        parser.setStream(new DataInputStream(fis));
        parser.parse();
        Set<Integer> allKnownFields = parser.getAllKnownFields();
        assertNotNull(allKnownFields);
        assertEquals(67, allKnownFields.size());
    }

    private byte[] pseudaHeader(byte[] content) {
        byte[] b = new byte[16 + content.length];
        ByteBuffer bb = ByteBuffer.wrap(b);
        bb.put((byte) 0x00);
        bb.put((byte) 0x0A);
        bb.putShort((short) (content.length + 16));
        bb.putInt((int) (new Date().getTime() / 1000));
        bb.putInt(0x88888888);
        bb.putInt(0xDDDDCCCC);
        bb.put(content, 0, content.length);
        return bb.array();
    }

    private byte[] buildTemplate(short id) {
        short length = 2 + 2 + 2 + 2 + 4 * 1;
        byte[] b = new byte[length];
        ByteBuffer bb = ByteBuffer.wrap(b);
        // set id
        bb.putShort((short) 0x0002);
        // length
        bb.putShort(length);
        // template id
        bb.putShort(id);
        // field count
        bb.putShort((short) 1);
        // add field
        bb.putShort((short) 1);
        bb.putShort((short) 2);

        return bb.array();
    }

    @Test
    public void testOutOfOrderTemplateDelivery() throws IOException, NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
        // build a template packet
        short templateId = (short) 0xbfbf;
        byte[] template = buildTemplate(templateId);
        // build a data packet
        short length = 4 + 2;
        byte[] b = new byte[length];
        ByteBuffer bb = ByteBuffer.wrap(b);
        bb.putShort(templateId);
        bb.putShort(length);
        bb.putShort((short) 0x1111);
        byte[] data = bb.array();

        byte[] d = pseudaHeader(data);
        byte[] e = pseudaHeader(template);

        logger.debug(String.format("Bytes (data) %s (%d)", new Object[] { Hex.encodeHexString(d), d.length }));
        logger.debug(String.format("Bytes (temp) %s (%d)", new Object[] { Hex.encodeHexString(e), e.length }));

        final NetflowParser parser = new NetflowParser();
        parser.setStream(new DataInputStream(new ByteArrayInputStream(d)));
        parser.parse();

        parser.setStream(new DataInputStream(new ByteArrayInputStream(e)));
        parser.parse();

        assertEquals(1, parser.getTemplates().size());
        assertNotNull(parser.getRecords());
        assertEquals(1, parser.getRecords().size());

        Field field = NetflowParser.class.getDeclaredField("readyToReRun");
        field.setAccessible(true);
        Object object = field.get(parser);
        int size = ((Queue) object).size();
        assertEquals(0, size);
    }

}
