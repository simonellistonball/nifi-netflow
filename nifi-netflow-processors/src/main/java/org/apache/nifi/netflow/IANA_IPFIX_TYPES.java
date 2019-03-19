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

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

public enum IANA_IPFIX_TYPES {
    BOOLEAN,
    // numbers, not floating points are handled as a special case
    unsigned64(8, true),
    unsigned32(4, true),
    unsigned16(2, true),
    unsigned8(1, true),
    signed64(8, false),
    signed32(4, false),
    signed16(2, false),
    signed8(1, false),
    float64,
    float32,
    // strings and special types that get special treatment
    string,
    octetArray,
    ipv4Address,
    ipv6Address,
    macAddress,
    // date and time related, which are treated as a number
    dateTimeSeconds(4, true, true),
    dateTimeMilliseconds(8, true, true),
    dateTimeMicroseconds(8, true, true),
    dateTimeNanoseconds(8, true, true),
    // only here as a holder for compatibility
    Netflow9,
    // extended structures
    basicList,
    subTemplateList,
    subTemplateMultiList;

    private final Charset UTF8_CHARSET = Charset.forName("UTF-8");

    public static long bytesToLong(byte[] b, int c) {
        long result = 0;
        if (c!=b.length) {c=b.length;}
        if (b.length!=0) {
        for (int i = 0; i < c; i++) {
            result <<= 8;
            result |= (b[i] & 0xFF);
        }}
        return result;
    }

    /**
     * Build a string representation of the byte array
     *
     * @throws UnknownHostException if a bad IP address is passed
     */
    public String convertToString(byte[] dst, int len) {
        if (this == ipv4Address) {
            try {
                return Inet4Address.getByAddress(dst).getHostAddress();
            } catch (UnknownHostException e) {
                return "";
            }
        } else if (this == ipv6Address) {
            try {
                return Inet6Address.getByAddress(dst).getHostAddress();
            } catch (UnknownHostException e) {
                return "";
            }
        } else if (this == IANA_IPFIX_TYPES.BOOLEAN) {
            if (dst[0] > 0)
                return "true";
            else
                return "false";
        } else if (this == IANA_IPFIX_TYPES.macAddress) {
            StringBuilder sb = new StringBuilder(18);
            for (byte b : dst) {
                if (sb.length() > 0)
                    sb.append(':');
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } else if (this == IANA_IPFIX_TYPES.float32) {
            return String.valueOf(ByteBuffer.wrap(dst).getFloat());
        } else if (this == IANA_IPFIX_TYPES.float64) {
            return String.valueOf(ByteBuffer.wrap(dst).getDouble());
        } else {
            if (bytes != 0) {
                // integer number, or date, note we pass a field length instead of using the
                // type's length, because IPFIX allows encoding of long types with shorter
                // representations
                // TOOD - do this properly!
                if (date) {
                    return String.valueOf(bytesToLong(dst, len > 0 ? len : bytes));
                } else {
                    if (signed) {
                        return String.valueOf(bytesToLong(dst, len > 0 ? len : bytes));
                    } else {
                        return String.valueOf(bytesToLong(dst, len > 0 ? len : bytes));
                    }

                }
            } else {
                return new String(dst, UTF8_CHARSET);
            }
        }
    }

    private int bytes = 0;
    private boolean date = false;
    private boolean signed = false;

    private IANA_IPFIX_TYPES(int bytes, boolean signed) {
        this.bytes = bytes;
        this.signed = signed;
    }

    private IANA_IPFIX_TYPES(int bytes, boolean signed, boolean date) {
        this.bytes = bytes;
        this.signed = signed;
        this.date = date;
    }

    private IANA_IPFIX_TYPES() {
    }

    public boolean isInteger() {
        return !date && bytes != 0;
    }

    public boolean isFloat() {
        return this == float32 || this == float64;
    }

    public boolean isDate() {
        return date;
    }
}
