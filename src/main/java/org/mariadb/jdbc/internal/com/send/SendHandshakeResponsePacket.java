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

package org.mariadb.jdbc.internal.com.send;

import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.MariaDbDatabaseMetaData;
import org.mariadb.jdbc.internal.MariaDbServerCapabilities;
import org.mariadb.jdbc.internal.com.read.Buffer;
import org.mariadb.jdbc.internal.com.read.ReadInitialHandShakePacket;
import org.mariadb.jdbc.internal.io.output.PacketOutputStream;
import org.mariadb.jdbc.internal.protocol.authentication.DefaultAuthenticationProvider;
import org.mariadb.jdbc.internal.util.Options;
import org.mariadb.jdbc.internal.util.PidFactory;
import org.mariadb.jdbc.internal.util.PidRequestInter;
import org.mariadb.jdbc.internal.util.Utils;
import org.mariadb.jdbc.internal.util.constant.Version;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.StringTokenizer;

/**
 * 4                            client_flags 4                            max_packet_size 1 charset_number 23 (filler)
 * always 0x00... n (Null-Terminated String)   user n (Length Coded Binary)      scramble_buff (1 + x bytes) 1 (filler)
 * always 0x00 n (Null-Terminated String) databasename
 * <p>
 * client_flags:            CLIENT_xxx options. The list of possible flag values is in the description of the Handshake
 * Initialisation Packet, for server_capabilities. For some of the bits, the server passed "what it's capable of". The
 * client leaves some of the bits on, adds others, and passes back to the server. One important flag is: whether
 * compression is desired.
 * <p>
 * max_packet_size:         the maximum number of bytes in a stream for the client
 * <p>
 * charset_number:          in the same domain as the server_language field that the server passes in the Handshake
 * Initialization stream.
 * <p>
 * user:                    identification
 * <p>
 * scramble_buff:           the password, after encrypting using the scramble_buff contents passed by the server (see
 * "Password functions" section elsewhere in this document) if length is zero, no password was given
 * <p>
 * databasename:            name of schema to use initially
 */
public class SendHandshakeResponsePacket {

    private static final PidRequestInter pidRequest;

    static {
        PidRequestInter init;
        try {
            init = PidFactory.getInstance();
        } catch (Throwable t) {
            init = () -> null;
        }
        pidRequest = init;
    }

    /**
     * Send handshake response packet.
     *
     * @param pos                output stream
     * @param username           user name
     * @param password           password
     * @param currentHost        current hostname
     * @param database           database name
     * @param clientCapabilities client capabilities
     * @param serverCapabilities server capabilities
     * @param serverLanguage     server language (utf8 / utf8mb4 collation)
     * @param packetSeq          packet sequence
     * @param options            user options
     * @param greetingPacket     server handshake packet information
     * @throws IOException if socket exception occur
     * @see <a href="https://mariadb.com/kb/en/mariadb/1-connecting-connecting/#handshake-response-packet">protocol documentation</a>
     */
    public static void send(final PacketOutputStream pos,
                            final String username,
                            final String password,
                            final HostAddress currentHost,
                            final String database,
                            final long clientCapabilities,
                            final long serverCapabilities,
                            final byte serverLanguage,
                            final byte packetSeq,
                            final Options options,
                            final ReadInitialHandShakePacket greetingPacket) throws IOException {

        pos.startPacket(packetSeq);

        final byte[] authData;
        switch (greetingPacket.getPluginName()) {
            case "": //CONJ-274 : permit connection mysql 5.1 db
            case DefaultAuthenticationProvider.MYSQL_NATIVE_PASSWORD:
                pos.permitTrace(false);
                try {
                    authData = Utils.encryptPassword(password, greetingPacket.getSeed(), options.passwordCharacterEncoding);
                    break;
                } catch (NoSuchAlgorithmException e) {
                    //cannot occur :
                    throw new IOException("Unknown algorithm SHA-1. Cannot encrypt password", e);
                }
            case DefaultAuthenticationProvider.MYSQL_CLEAR_PASSWORD:
                pos.permitTrace(false);
                if (options.passwordCharacterEncoding != null && !options.passwordCharacterEncoding.isEmpty()) {
                    authData = password.getBytes(options.passwordCharacterEncoding);
                } else {
                    authData = password.getBytes();
                }
                break;
            default:
                authData = new byte[0];
        }

        pos.writeInt((int) clientCapabilities);
        pos.writeInt(1024 * 1024 * 1024);
        pos.write(serverLanguage); //1

        pos.writeBytes((byte) 0, 19);    //19
        pos.writeInt((int) (clientCapabilities >> 32)); //Maria extended flag

        if (username == null || username.isEmpty()) {
            pos.write(System.getProperty("user.name").getBytes()); //to permit SSO
        } else {
            pos.write(username.getBytes());     //strlen username
        }

        pos.write((byte) 0);        //1

        if ((serverCapabilities & MariaDbServerCapabilities.PLUGIN_AUTH_LENENC_CLIENT_DATA) != 0) {
            pos.writeFieldLength(authData.length);
            pos.write(authData);
        } else if ((serverCapabilities & MariaDbServerCapabilities.SECURE_CONNECTION) != 0) {
            pos.write((byte) authData.length);
            pos.write(authData);
        } else {
            pos.write(authData);
            pos.write((byte) 0);
        }

        if ((serverCapabilities & MariaDbServerCapabilities.CONNECT_WITH_DB) != 0) {
            pos.write(database);
            pos.write((byte) 0);
        }

        if ((serverCapabilities & MariaDbServerCapabilities.PLUGIN_AUTH) != 0) {
            pos.write(greetingPacket.getPluginName());
            pos.write((byte) 0);
        }

        if ((serverCapabilities & MariaDbServerCapabilities.CONNECT_ATTRS) != 0) {
            writeConnectAttributes(pos, options.connectionAttributes, currentHost);
        }

        pos.flush();
        pos.permitTrace(true);
    }

    private static final byte[] _CLIENT_NAME = "_client_name".getBytes();
    private static final byte[] _CLIENT_VERSION = "_client_version".getBytes();
    private static final byte[] _SERVER_HOST = "_server_host".getBytes();
    private static final byte[] _OS = "_os".getBytes();
    private static final byte[] _PID = "_pid".getBytes();
    private static final byte[] _THREAD = "_thread".getBytes();
    private static final byte[] _JAVA_VENDOR = "_java_vendor".getBytes();
    private static final byte[] _JAVA_VERSION = "_java_version".getBytes();

    private static void writeConnectAttributes(PacketOutputStream pos, String connectionAttributes, HostAddress currentHost) throws IOException {
        Buffer buffer = new Buffer(new byte[200]);

        buffer.writeStringSmallLength(_CLIENT_NAME);
        buffer.writeStringLength(MariaDbDatabaseMetaData.DRIVER_NAME);

        buffer.writeStringSmallLength(_CLIENT_VERSION);
        buffer.writeStringLength(Version.version);

        buffer.writeStringSmallLength(_SERVER_HOST);
        buffer.writeStringLength( (currentHost != null) ? currentHost.host : "");

        buffer.writeStringSmallLength(_OS);
        buffer.writeStringLength(System.getProperty("os.name"));
        String pid = pidRequest.getPid();
        if (pid != null) {
            buffer.writeStringSmallLength(_PID);
            buffer.writeStringLength(pid);
        }

        buffer.writeStringSmallLength(_THREAD);
        buffer.writeStringLength(Long.toString(Thread.currentThread().getId()));

        buffer.writeStringLength(_JAVA_VENDOR);
        buffer.writeStringLength(System.getProperty("java.vendor"));

        buffer.writeStringSmallLength(_JAVA_VERSION);
        buffer.writeStringLength(System.getProperty("java.version"));

        if (connectionAttributes != null) {
            StringTokenizer tokenizer = new StringTokenizer(connectionAttributes, ",");
            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken();
                int separator = token.indexOf(":");
                if (separator != -1) {
                    buffer.writeStringLength(token.substring(0, separator));
                    buffer.writeStringLength(token.substring(separator + 1));
                } else {
                    buffer.writeStringLength(token);
                    buffer.writeStringLength("");
                }
            }
        }
        pos.writeFieldLength(buffer.position);
        pos.write(buffer.buf, 0, buffer.position);
    }

}
