package org.mariadb.jdbc.internal.util;

/*
MariaDB Client for Java

Copyright (c) 2012-2014 Monty Program Ab.

This library is free software; you can redistribute it and/or modify it under
the terms of the GNU Lesser General Public License as published by the Free
Software Foundation; either version 2.1 of the License, or (at your option)
any later version.

This library is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
for more details.

You should have received a copy of the GNU Lesser General Public License along
with this library; if not, write to Monty Program Ab info@montyprogram.com.

This particular MariaDB Client for Java file is work
derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
the following copyright and notice provisions:


Copyright (c) 2009-2011, Marcus Eriksson, Jay Pipes
Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:
Redistributions of source code must retain the above copyright notice, this list
of conditions and the following disclaimer.

Redistributions in binary form must reproduce the above copyright notice, this
list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

Neither the name of the driver nor the names of its contributors may not be
used to endorse or promote products derived from this software without specific
prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
OF SUCH DAMAGE.
*/

import org.mariadb.jdbc.UrlParser;
import org.mariadb.jdbc.internal.failover.FailoverProxy;
import org.mariadb.jdbc.internal.failover.impl.AuroraListener;
import org.mariadb.jdbc.internal.failover.impl.MastersFailoverListener;
import org.mariadb.jdbc.internal.failover.impl.MastersSlavesListener;
import org.mariadb.jdbc.internal.logging.ProtocolLoggingProxy;
import org.mariadb.jdbc.internal.protocol.AuroraProtocol;
import org.mariadb.jdbc.internal.protocol.MasterProtocol;
import org.mariadb.jdbc.internal.protocol.MastersSlavesProtocol;
import org.mariadb.jdbc.internal.protocol.Protocol;
import org.mariadb.jdbc.internal.socket.NamedPipeSocket;
import org.mariadb.jdbc.internal.socket.SharedMemorySocket;
import org.mariadb.jdbc.internal.socket.UnixDomainSocket;
import org.mariadb.jdbc.internal.util.dao.QueryException;

import javax.net.SocketFactory;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.TimeZone;
import java.util.concurrent.locks.ReentrantLock;


public class Utils {
    private static final char[] hexArray = "0123456789ABCDEF".toCharArray();

    /**
     * Escape String.
     *
     * @param value              value to escape
     * @param noBackslashEscapes must backslash be escaped
     * @return escaped string.
     */
    public static String escapeString(String value, boolean noBackslashEscapes) {
        if (value.indexOf("'") == -1) {
            if (noBackslashEscapes) {
                return value;
            }
            if (value.indexOf("\\") == -1) {
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
     * @param password the password to encrypt
     * @param seed     the seed to use
     * @return a scrambled password
     * @throws NoSuchAlgorithmException if SHA1 is not available on the platform we are using
     */
    public static byte[] encryptPassword(final String password, final byte[] seed) throws NoSuchAlgorithmException {
        if (password == null || password.equals("")) {
            return new byte[0];
        }

        final MessageDigest messageDigest = MessageDigest.getInstance("SHA-1");
        final byte[] stage1 = messageDigest.digest(password.getBytes());
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
    public static String replaceFunctionParameter(String functionString) {

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

        if (func.equals("convert") || func.equals("timestampdiff") || func.equals("timestampadd")) {
            String paramPrefix;

            if (func.equals("timestampdiff") || func.equals("timestampadd")) {
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
    public static String nativeSql(String sql, boolean noBackslashEscapes) throws SQLException {
        if (sql.indexOf('{') == -1) {
            return sql;
        }

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
     * @throws QueryException if any error occur during connection
     * @throws SQLException   if any error occur during connection
     */
    public static Protocol retrieveProxy(final UrlParser urlParser, final ReentrantLock lock) throws QueryException, SQLException {
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
                    socketFactory = (SocketFactory) (Class.forName(socketFactoryName).newInstance());
                } catch (Exception sfex) {
                    socketFactory = SocketFactory.getDefault();
                }
            } else {
                socketFactory = SocketFactory.getDefault();
            }
            return socketFactory.createSocket();
        }
    }

    /**
     * Hexdump.
     *
     * @param bytes             byte array
     * @param maxQuerySizeToLog max log size
     * @return String
     */
    public static String hexdump(byte[] bytes, int maxQuerySizeToLog) {
        return hexdump(bytes, maxQuerySizeToLog, 0, bytes.length);
    }

    /**
     * Hexdump.
     *
     * @param bytes             byte array
     * @param maxQuerySizeToLog max log size
     * @param offset            offset
     * @return String
     */
    public static String hexdump(byte[] bytes, int maxQuerySizeToLog, int offset) {
        return hexdump(bytes, maxQuerySizeToLog, offset, bytes.length);
    }

    /**
     * Hexdump.
     *
     * @param bytes             byte array
     * @param maxQuerySizeToLog max log size
     * @param offset            offset
     * @param length            length
     * @return String
     */
    public static String hexdump(byte[] bytes, int maxQuerySizeToLog, int offset, int length) {
        if (bytes.length <= offset) return "";
        int dataLength = Math.min(maxQuerySizeToLog, Math.min(bytes.length - offset, length));
        char[] hexChars = new char[dataLength * 2];
        for (int j = 0; j < dataLength; j++) {
            int byteValue = bytes[j + offset] & 0xFF;
            hexChars[j * 2] = hexArray[byteValue >>> 4];
            hexChars[j * 2 + 1] = hexArray[byteValue & 0x0F];
        }
        return new String(hexChars);
    }

}
