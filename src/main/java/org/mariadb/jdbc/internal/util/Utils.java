/*
 *
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2017 MariaDB Ab.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 * This particular MariaDB Client for Java file is work
 * derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
 * the following copyright and notice provisions:
 *
 * Copyright (c) 2009-2011, Marcus Eriksson
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 * Redistributions of source code must retain the above copyright notice, this list
 * of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice, this
 * list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * Neither the name of the driver nor the names of its contributors may not be
 * used to endorse or promote products derived from this software without specific
 * prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 */

package org.mariadb.jdbc.internal.util;

import org.mariadb.jdbc.UrlParser;
import org.mariadb.jdbc.internal.failover.FailoverProxy;
import org.mariadb.jdbc.internal.failover.impl.AuroraListener;
import org.mariadb.jdbc.internal.failover.impl.MastersFailoverListener;
import org.mariadb.jdbc.internal.failover.impl.MastersSlavesListener;
import org.mariadb.jdbc.internal.io.socket.NamedPipeSocket;
import org.mariadb.jdbc.internal.io.socket.SharedMemorySocket;
import org.mariadb.jdbc.internal.io.socket.UnixDomainSocket;
import org.mariadb.jdbc.internal.logging.ProtocolLoggingProxy;
import org.mariadb.jdbc.internal.protocol.AuroraProtocol;
import org.mariadb.jdbc.internal.protocol.MasterProtocol;
import org.mariadb.jdbc.internal.protocol.MastersSlavesProtocol;
import org.mariadb.jdbc.internal.protocol.Protocol;

import javax.net.SocketFactory;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Proxy;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.TimeZone;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;


@SuppressWarnings("Annotator")
public class Utils {
    private static final char[] hexArray = "0123456789ABCDEF".toCharArray();
    private static final Pattern IP_V4 = Pattern.compile("^(([1-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\\.){1}"
            + "(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\\.){2}"
            + "([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$");
    private static final Pattern IP_V6 = Pattern.compile("^[0-9a-fA-F]{1,4}(:[0-9a-fA-F]{1,4}){7}$");
    private static final Pattern IP_V6_COMPRESSED = Pattern.compile("^(([0-9A-Fa-f]{1,4}(:[0-9A-Fa-f]{1,4}){0,5})?)"
            + "::(([0-9A-Fa-f]{1,4}(:[0-9A-Fa-f]{1,4}){0,5})?)$");

    /**
     * Escape String.
     *
     * @param value              value to escape
     * @param noBackslashEscapes must backslash be escaped
     * @return escaped string.
     */
    public static String escapeString(String value, boolean noBackslashEscapes) {
        if (!value.contains("'")) {
            if (noBackslashEscapes) {
                return value;
            }
            if (!value.contains("\\")) {
                return value;
            }
        }
        String escaped = value.replace("'", "''");
        if (noBackslashEscapes) {
            return escaped;
        }
        return escaped.replace("\\", "\\\\");
    }

    /**
     * encrypts a password
     * <p>
     * protocol for authentication is like this: 1. mysql server sends a random array of bytes (the seed) 2. client
     * makes a sha1 digest of the password 3. client hashes the output of 2 4. client digests the seed 5. client updates
     * the digest with the output from 3 6. an xor of the output of 5 and 2 is sent to server 7. server does the same
     * thing and verifies that the scrambled passwords match
     *
     * @param password                  the password to encrypt
     * @param seed                      the seed to use
     * @param passwordCharacterEncoding password character encoding
     * @return a scrambled password
     * @throws NoSuchAlgorithmException     if SHA1 is not available on the platform we are using
     * @throws UnsupportedEncodingException if passwordCharacterEncoding is not a valid charset name
     */
    public static byte[] encryptPassword(final String password, final byte[] seed, String passwordCharacterEncoding)
            throws NoSuchAlgorithmException, UnsupportedEncodingException {

        if (password == null || password.equals("")) return new byte[0];

        final MessageDigest messageDigest = MessageDigest.getInstance("SHA-1");
        byte[] bytePwd;
        if (passwordCharacterEncoding != null && !passwordCharacterEncoding.isEmpty()) {
            bytePwd = password.getBytes(passwordCharacterEncoding);
        } else {
            bytePwd = password.getBytes();
        }

        final byte[] stage1 = messageDigest.digest(bytePwd);
        messageDigest.reset();

        final byte[] stage2 = messageDigest.digest(stage1);
        messageDigest.reset();

        messageDigest.update(seed);
        messageDigest.update(stage2);

        final byte[] digest = messageDigest.digest();
        final byte[] returnBytes = new byte[digest.length];
        for (int i = 0; i < digest.length; i++) {
            returnBytes[i] = (byte) (stage1[i] ^ digest[i]);
        }
        return returnBytes;
    }

    /**
     * Copies the original byte array content to a new byte array. The resulting byte array is
     * always "length" size. If length is smaller than the original byte array, the resulting
     * byte array is truncated. If length is bigger than the original byte array, the resulting
     * byte array is filled with zero bytes.
     *
     * @param orig   the original byte array
     * @param length how big the resulting byte array will be
     * @return the copied byte array
     */
    public static byte[] copyWithLength(byte[] orig, int length) {
        // No need to initialize with zero bytes, because the bytes are already initialized with that
        byte[] result = new byte[length];
        int howMuchToCopy = length < orig.length ? length : orig.length;
        System.arraycopy(orig, 0, result, 0, howMuchToCopy);
        return result;
    }

    /**
     * Copies from original byte array to a new byte array. The resulting byte array is
     * always "to-from" size.
     *
     * @param orig the original byte array
     * @param from index of first byte in original byte array which will be copied
     * @param to   index of last byte in original byte array which will be copied. This can be
     *             outside of the original byte array
     * @return resulting array
     */
    public static byte[] copyRange(byte[] orig, int from, int to) {
        int length = to - from;
        byte[] result = new byte[length];
        int howMuchToCopy = orig.length - from < length ? orig.length - from : length;
        System.arraycopy(orig, from, result, 0, howMuchToCopy);
        return result;
    }

    /**
     * Helper function to replace function parameters in escaped string.
     * 3 functions are handles :
     * - CONVERT(value, type) , we replace SQL_XXX types with XXX, i.e SQL_INTEGER with INTEGER
     * - TIMESTAMPDIFF(type, ...) or TIMESTAMPADD(type, ...) , we replace SQL_TSI_XXX in type with XXX, i.e
     * SQL_TSI_HOUR with HOUR
     *
     * @param functionString - input string
     * @return unescaped string
     */
    private static String replaceFunctionParameter(String functionString) {

        if (!functionString.contains("SQL_")) {
            return functionString;
        }

        char[] input = functionString.toCharArray();
        StringBuilder sb = new StringBuilder();
        int index;
        for (index = 0; index < input.length; index++) {
            if (input[index] != ' ') {
                break;
            }
        }

        for (; ((input[index] >= 'a' && index <= 'z') || (input[index] >= 'A' && input[index] <= 'Z')) && index < input.length; index++) {
            sb.append(input[index]);
        }
        String func = sb.toString().toLowerCase();

        if ("convert".equals(func) || "timestampdiff".equals(func) || "timestampadd".equals(func)) {
            String paramPrefix;

            if ("timestampdiff".equals(func) || "timestampadd".equals(func)) {
                // Skip to first parameter
                for (; index < input.length; index++) {
                    if (!Character.isWhitespace(input[index]) && input[index] != '(') {
                        break;
                    }
                }
                if (index == input.length) {
                    return new String(input);
                }


                if (index >= input.length - 8) {
                    return new String(input);
                }
                paramPrefix = new String(input, index, 8);
                if (paramPrefix.equals("SQL_TSI_")) {
                    return new String(input, 0, index) + new String(input, index + 8, input.length - (index + 8));
                }
                return new String(input);
            }

            // Handle "convert(value, type)" case
            // extract last parameter, after the last ','
            int lastCommaIndex = functionString.lastIndexOf(',');

            for (index = lastCommaIndex + 1; index < input.length; index++) {
                if (!Character.isWhitespace(input[index])) {
                    break;
                }
            }
            if (index >= input.length - 4) {
                return new String(input);
            }
            paramPrefix = new String(input, index, 4);
            if (paramPrefix.equals("SQL_")) {
                return new String(input, 0, index) + new String(input, index + 4, input.length - (index + 4));
            }

        }
        return new String(input);
    }

    private static String resolveEscapes(String escaped, boolean noBackslashEscapes) throws SQLException {
        if (escaped.charAt(0) != '{' || escaped.charAt(escaped.length() - 1) != '}') {
            throw new SQLException("unexpected escaped string");
        }
        int endIndex = escaped.length() - 1;
        String escapedLower = escaped.toLowerCase();
        if (escaped.startsWith("{fn ")) {
            String resolvedParams = replaceFunctionParameter(escaped.substring(4, endIndex));
            return nativeSql(resolvedParams, noBackslashEscapes);
        } else if (escapedLower.startsWith("{oj ")) {
            // Outer join
            // the server supports "oj" in any case, even "oJ"
            return nativeSql(escaped.substring(4, endIndex), noBackslashEscapes);
        } else if (escaped.startsWith("{d ")) {
            // date literal
            return escaped.substring(3, endIndex);
        } else if (escaped.startsWith("{t ")) {
            // time literal
            return escaped.substring(3, endIndex);
        } else if (escaped.startsWith("{ts ")) {
            //timestamp literal
            return escaped.substring(4, endIndex);
        } else if (escaped.startsWith("{d'")) {
            // date literal, no space
            return escaped.substring(2, endIndex);
        } else if (escaped.startsWith("{t'")) {
            // time literal
            return escaped.substring(2, endIndex);
        } else if (escaped.startsWith("{ts'")) {
            //timestamp literal
            return escaped.substring(3, endIndex);
        } else if (escaped.startsWith("{call ") || escaped.startsWith("{CALL ")) {
            // We support uppercase "{CALL" only because Connector/J supports it. It is not in the JDBC spec.

            return nativeSql(escaped.substring(1, endIndex), noBackslashEscapes);
        } else if (escaped.startsWith("{escape ")) {
            return escaped.substring(1, endIndex);
        } else if (escaped.startsWith("{?")) {
            // likely ?=call(...)
            return nativeSql(escaped.substring(1, endIndex), noBackslashEscapes);
        } else if (escaped.startsWith("{ ")) {
            // Spaces before keyword, this is not JDBC compliant, however some it works in some drivers,
            // so we support it, too
            for (int i = 2; i < escaped.length(); i++) {
                if (!Character.isWhitespace(escaped.charAt(i))) {
                    return resolveEscapes("{" + escaped.substring(i), noBackslashEscapes);
                }
            }
        }
        throw new SQLException("unknown escape sequence " + escaped);
    }

    /**
     * Escape sql String
     *
     * @param sql                initial sql
     * @param noBackslashEscapes must backslash be escape
     * @return escaped sql string
     * @throws SQLException if escape sequence is incorrect.
     */
    @SuppressWarnings("ConstantConditions")
    public static String nativeSql(String sql, boolean noBackslashEscapes) throws SQLException {
        if (sql.indexOf('{') == -1) return sql;

        StringBuilder escapeSequenceBuf = new StringBuilder();
        StringBuilder sqlBuffer = new StringBuilder();

        char[] charArray = sql.toCharArray();
        char lastChar = 0;
        boolean inQuote = false;
        char quoteChar = 0;
        boolean inComment = false;
        boolean isSlashSlashComment = false;
        int inEscapeSeq = 0;

        for (int i = 0; i < charArray.length; i++) {
            char car = charArray[i];
            if (lastChar == '\\' && !noBackslashEscapes) {
                sqlBuffer.append(car);
                continue;
            }

            switch (car) {
                case '\'':
                case '"':
                    if (!inComment) {
                        if (inQuote) {
                            if (quoteChar == car) {
                                inQuote = false;
                            }
                        } else {
                            inQuote = true;
                            quoteChar = car;
                        }
                    }
                    break;

                case '*':
                    if (!inQuote && !inComment && lastChar == '/') {
                        inComment = true;
                        isSlashSlashComment = false;
                    }
                    break;
                case '/':
                case '-':
                    if (!inQuote) {
                        if (inComment) {
                            if (lastChar == '*' && !isSlashSlashComment) {
                                inComment = false;
                            } else if (lastChar == car && isSlashSlashComment) {
                                inComment = false;
                            }
                        } else {
                            if (lastChar == car) {
                                inComment = true;
                                isSlashSlashComment = true;
                            } else if (lastChar == '*') {
                                inComment = true;
                                isSlashSlashComment = false;
                            }
                        }
                    }
                    break;
                case 'S':
                    // skip SQL_xxx and SQL_TSI_xxx in functions
                    // This would convert e.g SQL_INTEGER => INTEGER, SQL_TSI_HOUR=>HOUR

                    if (!inQuote && !inComment && inEscapeSeq > 0
                            && i + 4 < charArray.length && charArray[i + 1] == 'Q'
                            && charArray[i + 2] == 'L' && charArray[i + 3] == 'L'
                            && charArray[i + 4] == '_') {

                        if (i + 8 < charArray.length
                                && charArray[i + 5] == 'T'
                                && charArray[i + 6] == 'S'
                                && charArray[i + 7] == 'I'
                                && charArray[i + 8] == '_') {
                            i += 8;
                            continue;
                        }
                        i += 4;
                        continue;
                    }
                    break;
                case '\n':
                    if (inComment && isSlashSlashComment) {
                        // slash-slash and dash-dash comments ends with the end of line
                        inComment = false;
                    }
                    break;
                case '{':
                    if (!inQuote && !inComment) {
                        inEscapeSeq++;
                    }
                    break;

                case '}':
                    if (!inQuote && !inComment) {
                        inEscapeSeq--;
                        if (inEscapeSeq == 0) {
                            escapeSequenceBuf.append(car);
                            sqlBuffer.append(resolveEscapes(escapeSequenceBuf.toString(), noBackslashEscapes));
                            escapeSequenceBuf.setLength(0);
                            continue;
                        }
                    }
                    break;

                default:
                    break;

            }
            lastChar = car;
            if (inEscapeSeq > 0) {
                escapeSequenceBuf.append(car);
            } else {
                sqlBuffer.append(car);
            }
        }
        if (inEscapeSeq > 0) {
            throw new SQLException("Invalid escape sequence , missing closing '}' character in '" + sqlBuffer);
        }
        return sqlBuffer.toString();
    }

    /**
     * Retrieve protocol corresponding to the failover options.
     * if no failover option, protocol will not be proxied.
     * if a failover option is precised, protocol will be proxied so that any connection error will be handle directly.
     *
     * @param urlParser urlParser corresponding to connection url string.
     * @param lock      lock to handle thread synchronisation
     * @return protocol
     * @throws SQLException if any error occur during connection
     */
    public static Protocol retrieveProxy(final UrlParser urlParser, final ReentrantLock lock) throws SQLException {
        Protocol protocol;
        switch (urlParser.getHaMode()) {
            case AURORA:
                return getProxyLoggingIfNeeded(urlParser, (Protocol) Proxy.newProxyInstance(
                        AuroraProtocol.class.getClassLoader(),
                        new Class[]{Protocol.class},
                        new FailoverProxy(new AuroraListener(urlParser), lock)));
            case REPLICATION:
                return getProxyLoggingIfNeeded(urlParser,
                        (Protocol) Proxy.newProxyInstance(
                                MastersSlavesProtocol.class.getClassLoader(),
                                new Class[]{Protocol.class},
                                new FailoverProxy(new MastersSlavesListener(urlParser), lock)));
            case FAILOVER:
            case SEQUENTIAL:
                return getProxyLoggingIfNeeded(urlParser, (Protocol) Proxy.newProxyInstance(
                        MasterProtocol.class.getClassLoader(),
                        new Class[]{Protocol.class},
                        new FailoverProxy(new MastersFailoverListener(urlParser), lock)));
            default:
                protocol = getProxyLoggingIfNeeded(urlParser, new MasterProtocol(urlParser, lock));
                protocol.connectWithoutProxy();
                return protocol;
        }
    }

    private static Protocol getProxyLoggingIfNeeded(UrlParser urlParser, Protocol protocol) {
        if (urlParser.getOptions().profileSql || urlParser.getOptions().slowQueryThresholdNanos != null) {
            return (Protocol) Proxy.newProxyInstance(
                    MasterProtocol.class.getClassLoader(),
                    new Class[]{Protocol.class},
                    new ProtocolLoggingProxy(protocol, urlParser.getOptions()));
        }
        return protocol;
    }

    /**
     * Get timezone from Id.
     * This differ from java implementation : by default, if timezone Id is unknown, java return GMT timezone.
     * GMT will be return only if explicitly asked.
     *
     * @param id timezone id
     * @return timezone.
     * @throws SQLException if no timezone is found for this Id
     */
    public static TimeZone getTimeZone(String id) throws SQLException {
        TimeZone tz = TimeZone.getTimeZone(id);

        // Validate the timezone ID. JDK maps invalid timezones to GMT
        if (tz.getID().equals("GMT") && !id.equals("GMT")) {
            throw new SQLException("invalid timezone id '" + id + "'");
        }
        return tz;
    }

    /**
     * Create socket accordingly to options.
     *
     * @param urlParser urlParser
     * @param host      hostName ( mandatory only for named pipe)
     * @return a nex socket
     * @throws IOException if connection error occur
     */
    @SuppressWarnings("unchecked")
    public static Socket createSocket(UrlParser urlParser, String host) throws IOException {

        if (urlParser.getOptions().pipe != null) {
            return new NamedPipeSocket(host, urlParser.getOptions().pipe);
        } else if (urlParser.getOptions().localSocket != null) {
            try {
                return new UnixDomainSocket(urlParser.getOptions().localSocket);
            } catch (RuntimeException re) {
                throw new IOException(re.getMessage(), re.getCause());
            }
        } else if (urlParser.getOptions().sharedMemory != null) {
            try {
                return new SharedMemorySocket(urlParser.getOptions().sharedMemory);
            } catch (RuntimeException re) {
                throw new IOException(re.getMessage(), re.getCause());
            }
        } else {
            SocketFactory socketFactory;
            String socketFactoryName = urlParser.getOptions().socketFactory;
            if (socketFactoryName != null) {
                try {
                    Class<? extends SocketFactory> socketFactoryClass = (Class<? extends SocketFactory>) Class.forName(socketFactoryName);
                    if (socketFactoryClass != null) {
                        Constructor<? extends SocketFactory> constructor = socketFactoryClass.getConstructor();
                        socketFactory = constructor.newInstance();
                        return socketFactory.createSocket();
                    }
                } catch (Exception exp) {
                    throw new IOException("Socket factory failed to initialized with option \"socketFactory\" set to \""
                            + urlParser.getOptions().socketFactory + "\"", exp);
                }
            }
            socketFactory = SocketFactory.getDefault();
            return socketFactory.createSocket();
        }
    }

    /**
     * Hexdump.
     *
     * @param bytes byte arrays
     * @return String
     */
    public static String hexdump(byte[]... bytes) {
        return hexdump(Integer.MAX_VALUE, 0, Integer.MAX_VALUE, bytes);
    }

    /**
     * Hexdump.
     * <p>
     * String output example :
     * <pre>
     * {@code
     *    7D 00 00 01 C5 00 00                                 }......            &lt;- first byte array
     *    01 00 00 01 02 33 00 00  02 03 64 65 66 05 74 65     .....3....def.te   &lt;- second byte array
     *    73 74 6A 0A 74 65 73 74  5F 62 61 74 63 68 0A 74     stj.test_batch.t
     *    65 73 74 5F 62 61 74 63  68 02 69 64 02 69 64 0C     est_batch.id.id.
     *    3F 00 0B 00 00 00 03 03  42 00 00 00 37 00 00 03     ?.......B...7...
     *    03 64 65 66 05 74 65 73  74 6A 0A 74 65 73 74 5F     .def.testj.test_
     *    62 61 74 63 68 0A 74 65  73 74 5F 62 61 74 63 68     batch.test_batch
     *    04 74 65 73 74 04 74 65  73 74 0C 21 00 1E 00 00     .test.test.!....
     *    00 FD 00 00 00 00 00 05  00 00 04 FE 00 00 22 00     ..............".
     *    06 00 00 05 01 31 03 61  61 61 06 00 00 06 01 32     .....1.aaa.....2
     *    03 62 62 62 06 00 00 07  01 33 03 63 63 63 06 00     .bbb.....3.ccc..
     *    00 08 01 34 03 61 61 61  06 00 00 09 01 35 03 62     ...4.aaa.....5.b
     *    62 62 06 00 00 0A 01 36  03 63 63 63 05 00 00 0B     bb.....6.ccc....
     *    FE 00 00 22 00                                       ...".
     * }
     * </pre>
     *
     * @param maxQuerySizeToLog max log size
     * @param offset            offset of last byte array
     * @param length            length of last byte array
     * @param byteArr           byte arrays. if many, only the last may have offset and size limitation
     *                          others will be displayed completely.
     * @return String
     */
    public static String hexdump(int maxQuerySizeToLog, int offset, int length, byte[]... byteArr) {
        switch (byteArr.length) {
            case 0:
                return "";

            case 1:
                byte[] bytes = byteArr[0];
                if (bytes.length <= offset) return "";
                int dataLength = Math.min(maxQuerySizeToLog, Math.min(bytes.length - offset, length));

                StringBuilder outputBuilder = new StringBuilder(dataLength * 5);
                outputBuilder.append("\n");
                writeHex(bytes, offset, dataLength, outputBuilder);

            default:
                StringBuilder sb = new StringBuilder();
                sb.append("\n");
                byte[] arr;
                for (int i = 0; i < byteArr.length - 1; i++) {
                    arr = byteArr[i];
                    writeHex(arr, 0, arr.length, sb);
                }
                arr = byteArr[byteArr.length - 1];
                int dataLength2 = Math.min(maxQuerySizeToLog, Math.min(arr.length - offset, length));
                writeHex(arr, offset, dataLength2, sb);
                return sb.toString();

        }
    }

    /**
     * Write bytes/hexadecimal value of a byte array to a StringBuilder.
     * <p>
     * String output example :
     * <pre>
     * {@code
     * 38 00 00 00 03 63 72 65  61 74 65 20 74 61 62 6C     8....create tabl
     * 65 20 42 6C 6F 62 54 65  73 74 63 6C 6F 62 74 65     e BlobTestclobte
     * 73 74 32 20 28 73 74 72  6D 20 74 65 78 74 29 20     st2 (strm text)
     * 43 48 41 52 53 45 54 20  75 74 66 38                 CHARSET utf8
     * }
     * </pre>
     *
     * @param bytes         byte array
     * @param offset        offset
     * @param dataLength    byte length to write
     * @param outputBuilder string builder
     */
    private static void writeHex(byte[] bytes, int offset, int dataLength, StringBuilder outputBuilder) {

        if (bytes == null || bytes.length == 0) return;

        char[] hexaValue = new char[16];
        hexaValue[8] = ' ';

        int pos = offset;
        int posHexa = 0;

        while (pos < dataLength) {
            int byteValue = bytes[pos] & 0xFF;
            outputBuilder.append(hexArray[byteValue >>> 4])
                    .append(hexArray[byteValue & 0x0F])
                    .append(" ");

            hexaValue[posHexa++] = (byteValue > 31 && byteValue < 127) ? (char) byteValue : '.';

            if (posHexa == 8) {
                outputBuilder.append(" ");
            }

            if (posHexa == 16) {
                outputBuilder.append("    ")
                        .append(hexaValue)
                        .append("\n");
                posHexa = 0;
            }

            pos++;
        }

        int remaining = posHexa;
        if (remaining > 0) {
            if (remaining < 8) {
                for (; remaining < 8; remaining++) outputBuilder.append("   ");
                outputBuilder.append(" ");
            }

            for (; remaining < 16; remaining++) outputBuilder.append("   ");

            outputBuilder.append("    ")
                    .append(hexaValue, 0, posHexa)
                    .append("\n");
        }
    }

    private static String getHex(final byte[] raw) {
        final StringBuilder hex = new StringBuilder(2 * raw.length);
        for (final byte b : raw) {
            hex.append(hexArray[(b & 0xF0) >> 4])
                    .append(hexArray[(b & 0x0F)]);
        }
        return hex.toString();
    }

    public static String byteArrayToHexString(final byte[] bytes) {
        return (bytes != null) ? getHex(bytes) : "";
    }

    /**
     * Parse the option "sessionVariable" to ensure having no injection.
     * semi-column not in string will be replaced by comma.
     *
     * @param sessionVariable option value
     * @return parsed String
     */
    public static String parseSessionVariables(String sessionVariable) {
        StringBuilder out = new StringBuilder();
        StringBuilder sb = new StringBuilder();
        Parse state = Parse.Normal;
        boolean iskey = true;
        boolean singleQuotes = true;
        boolean first = true;
        String key = null;

        char[] chars = sessionVariable.toCharArray();

        for (char car : chars) {

            if (state == Parse.Escape) {
                sb.append(car);
                state = singleQuotes ? Parse.Quote : Parse.String;
                continue;
            }

            switch (car) {
                case '"':
                    if (state == Parse.Normal) {
                        state = Parse.String;
                        singleQuotes = false;
                    } else if (state == Parse.String && !singleQuotes) {
                        state = Parse.Normal;
                    }
                    break;

                case '\'':
                    if (state == Parse.Normal) {
                        state = Parse.String;
                        singleQuotes = true;
                    } else if (state == Parse.String && singleQuotes) {
                        state = Parse.Normal;
                    }
                    break;

                case '\\':
                    if (state == Parse.String) state = Parse.Escape;
                    break;

                case ';':
                case ',':
                    if (state == Parse.Normal) {
                        if (!iskey) {
                            if (!first) out.append(",");
                            out.append(key);
                            out.append(sb.toString());
                            first = false;
                        } else {
                            key = sb.toString().trim();
                            if (!key.isEmpty()) {
                                if (!first) out.append(",");
                                out.append(key);
                                first = false;
                            }
                        }
                        iskey = true;
                        key = null;
                        sb = new StringBuilder();
                        continue;
                    }
                    break;

                case '=':
                    if (state == Parse.Normal && iskey) {
                        key = sb.toString().trim();
                        iskey = false;
                        sb = new StringBuilder();
                    }
                    break;

                default:
                    //nothing
            }

            sb.append(car);
        }

        if (!iskey) {
            if (!first) out.append(",");
            out.append(key);
            out.append(sb.toString());
        } else {
            String tmpkey = sb.toString().trim();
            if (!tmpkey.isEmpty() && !first) out.append(",");
            out.append(tmpkey);
        }
        return out.toString();
    }

    public static boolean isIPv4(final String ip) {
        return IP_V4.matcher(ip).matches();
    }

    public static boolean isIPv6(final String ip) {
        return IP_V6.matcher(ip).matches() || IP_V6_COMPRESSED.matcher(ip).matches();
    }

    private enum Parse {
        Normal,
        String, /* inside string */
        Quote,
        Escape /* found backslash */
    }

}
