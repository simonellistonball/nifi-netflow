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
import java.util.Set;

import org.junit.Test;

public class NetflowParserTest {

    @Test
    public void testParser() throws IOException {
        final InputStream fis = new FileInputStream(new File("src/test/resources/example.ipfix"));
        final NetflowParser parser = new NetflowParser(new DataInputStream(fis));
        parser.parse();
        assertEquals(10,  parser.getTemplates().size());
        assertNotNull(parser.getRecords());
        assertEquals(35, parser.getRecords().size());
    }

    @Test
    public void testTheUniqueFieldBuilder() throws IOException {
        final InputStream fis = new FileInputStream(new File("src/test/resources/example.ipfix"));
        final NetflowParser parser = new NetflowParser(new DataInputStream(fis));
        parser.parse();
        Set<Integer> allKnownFields = parser.getAllKnownFields();
        assertNotNull(allKnownFields);
        assertEquals(67, allKnownFields.size());
    }

}