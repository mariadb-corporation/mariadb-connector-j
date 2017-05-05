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

Copyright (c) 2009-2011, Marcus Eriksson

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

package org.mariadb.jdbc.internal.com.send;

import org.mariadb.jdbc.MariaDbConnection;
import org.mariadb.jdbc.MariaDbDatabaseMetaData;
import org.mariadb.jdbc.internal.MariaDbServerCapabilities;
import org.mariadb.jdbc.internal.com.read.Buffer;
import org.mariadb.jdbc.internal.protocol.AbstractConnectProtocol;
import org.mariadb.jdbc.internal.protocol.AuroraProtocol;
import org.mariadb.jdbc.internal.protocol.authentication.DefaultAuthenticationProvider;
import org.mariadb.jdbc.internal.io.output.PacketOutputStream;
import org.mariadb.jdbc.internal.util.Options;
import org.mariadb.jdbc.internal.util.PidFactory;
import org.mariadb.jdbc.internal.util.Utils;
import org.mariadb.jdbc.internal.util.constant.HaMode;
import org.mariadb.jdbc.internal.util.constant.Version;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.StringTokenizer;

import static org.mariadb.jdbc.internal.com.Packet.*;

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

    /**
     * Send handshake response packet.
     *
     * @see <a href="https://mariadb.com/kb/en/mariadb/1-connecting-connecting/#handshake-response-packet">protocol documentation</a>
     * @param pos                       output stream
     * @param username                  user name
     * @param password                  password
     * @param database                  database name
     * @param clientCapabilities        client capabilities
     * @param serverCapabilities        server capabilities
     * @param serverLanguage            server language (utf8 / utf8mb4 collation)
     * @param seed                      seed
     * @param packetSeq                 packet sequence
     * @param plugin                    plugin name
     * @param options                   user options
     * @param haMode                    High availability mode
     * @throws IOException if socket exception occur
     */
    public static void send(final PacketOutputStream pos,
                            String username,
                            final String password,
                            final String database,
                            final long clientCapabilities,
                            final long serverCapabilities,
                            final byte serverLanguage,
                            final byte[] seed,
                            final byte packetSeq,
                            final String plugin,
                            final Options options,
                            final HaMode haMode) throws IOException {

        pos.startPacket(packetSeq);

        final byte[] authData;
        switch (plugin) {
            case "": //CONJ-274 : permit connection mysql 5.1 db
            case DefaultAuthenticationProvider.MYSQL_NATIVE_PASSWORD:
                pos.permitTrace(false);
                try {
                    authData = Utils.encryptPassword(password, seed, options.passwordCharacterEncoding);
                    break;
                } catch (NoSuchAlgorithmException e) {
                    throw new RuntimeException("Could not use SHA-1, failing", e);
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

        if (username == null || "".equals(username)) username = System.getProperty("user.name"); //permit SSO

        pos.write(username.getBytes());     //strlen username
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
            pos.write(plugin);
            pos.write((byte) 0);
        }

        if ((serverCapabilities & MariaDbServerCapabilities.CONNECT_ATTRS) != 0) {
            writeConnectAttributes(pos, options.connectionAttributes);
        }

        if ((serverCapabilities & MariaDbServerCapabilities.MARIADB_CLIENT_COM_IN_AUTH) != 0) {
            writeAdditionalQueries(pos, serverCapabilities, options, haMode, database);
        }
        pos.flush();
        pos.permitTrace(true);
    }

    private static void writeAdditionalQueries(PacketOutputStream pos, long serverCapabilities, Options options, HaMode haMode, String database)
            throws IOException {

        //reservation for authentication length
        Buffer buffer = new Buffer(new byte[200]);

        //send variables command
        StringBuilder sessionOption = new StringBuilder("set autocommit=1");
        if ((serverCapabilities & MariaDbServerCapabilities.CLIENT_SESSION_TRACK) != 0) {
            sessionOption.append(", session_track_schema=1");
            if (options.rewriteBatchedStatements) {
                sessionOption.append(", session_track_system_variables='auto_increment_increment' ");
            }
        }
        if (options.jdbcCompliantTruncation) {
            sessionOption.append(", sql_mode = concat(@@sql_mode,',STRICT_TRANS_TABLES')");
        }
        if (options.sessionVariables != null) sessionOption.append("," + options.sessionVariables);
        buffer.writeBytes(COM_QUERY, sessionOption.toString().getBytes(StandardCharsets.UTF_8));

        //ask for session variables
        buffer.writeBytes(COM_QUERY, AbstractConnectProtocol.SESSION_QUERY);

        //ask if server is read-only
        if (haMode == HaMode.AURORA) {
            buffer.writeBytes(COM_QUERY, AuroraProtocol.IS_MASTER_QUERY);
        }

        if (options.createDatabaseIfNotExist) {
            // Try to create the database if it does not exist
            String quotedDb = MariaDbConnection.quoteIdentifier(database);
            buffer.writeBytes(COM_QUERY, ("CREATE DATABASE IF NOT EXISTS " + quotedDb).getBytes(StandardCharsets.UTF_8));
            buffer.writeBytes(COM_QUERY, ("USE " + quotedDb).getBytes(StandardCharsets.UTF_8));
        }

        pos.write(buffer.position + 1); //COM_MULTI packet length
        pos.write(COM_MULTI);
        pos.write(buffer.buf, 0, buffer.position);

    }

    private static void writeConnectAttributes(PacketOutputStream pos, String connectionAttributes) throws IOException {
        Buffer buffer = new Buffer(new byte[200]);

        buffer.writeStringLength("_client_name");
        buffer.writeStringLength(MariaDbDatabaseMetaData.DRIVER_NAME);

        buffer.writeStringLength("_client_version");
        buffer.writeStringLength(Version.version);

        buffer.writeStringLength("_os");
        buffer.writeStringLength(System.getProperty("os.name"));

        String pid = PidFactory.getInstance().getPid();
        if (pid != null) {
            buffer.writeStringLength("_pid");
            buffer.writeStringLength(pid);
        }

        buffer.writeStringLength("_thread");
        buffer.writeStringLength(Long.toString(Thread.currentThread().getId()));

        buffer.writeStringLength("_java_vendor");
        buffer.writeStringLength(System.getProperty("java.vendor"));

        buffer.writeStringLength("_java_version");
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
