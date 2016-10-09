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

package org.mariadb.jdbc.internal.packet.send;

import org.mariadb.jdbc.MariaDbDatabaseMetaData;
import org.mariadb.jdbc.internal.MariaDbServerCapabilities;
import org.mariadb.jdbc.internal.protocol.authentication.DefaultAuthenticationProvider;
import org.mariadb.jdbc.internal.util.PidFactory;
import org.mariadb.jdbc.internal.util.Utils;
import org.mariadb.jdbc.internal.stream.PacketOutputStream;
import org.mariadb.jdbc.internal.util.constant.Version;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
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
public class SendHandshakeResponsePacket implements InterfaceSendPacket {
    private byte packetSeq;
    private String username;
    private String password;
    private byte[] seed;
    private long clientCapabilities;
    private final byte serverLanguage;
    private String database;
    private String plugin;
    private String connectionAttributes;
    private long serverThreadId;

    private byte[] connectionAttributesArray;
    private int connectionAttributesPosition;

    /**
     * Initialisation of parameters.
     * @param username username
     * @param password user password
     * @param database initial database connection
     * @param clientCapabilities capabilities
     * @param serverLanguage serverlanguage
     * @param seed seed
     * @param packetSeq stream sequence
     * @param plugin authentication plugin name
     * @param connectionAttributes connection attributes option
     * @param serverThreadId threadId;
     */
    public SendHandshakeResponsePacket(final String username,
                                       final String password,
                                       final String database,
                                       final long clientCapabilities,
                                       final byte serverLanguage,
                                       final byte[] seed,
                                       byte packetSeq,
                                       String plugin,
                                       String connectionAttributes,
                                       long serverThreadId) {
        this.packetSeq = packetSeq;
        this.username = username;
        this.password = password;
        this.seed = seed;
        this.clientCapabilities = clientCapabilities;
        this.serverLanguage = serverLanguage;
        this.database = database;
        this.plugin = plugin;
        this.connectionAttributes = connectionAttributes;
        this.serverThreadId = serverThreadId;
    }

    /**
     * Send authentication stream.
     * @param os database socket
     * @throws IOException if any connection error occur
     */
    public void send(final OutputStream os) throws IOException {
        PacketOutputStream writeBuffer = (PacketOutputStream) os;
        writeBuffer.startPacket(packetSeq);
        final byte[] authData;
        switch (plugin) {
            case "": //CONJ-274 : permit connection mysql 5.1 db
            case DefaultAuthenticationProvider.MYSQL_NATIVE_PASSWORD :
                try {
                    authData = Utils.encryptPassword(password, seed);
                    break;
                } catch (NoSuchAlgorithmException e) {
                    throw new RuntimeException("Could not use SHA-1, failing", e);
                }
            case DefaultAuthenticationProvider.MYSQL_CLEAR_PASSWORD :
                authData = password.getBytes();
                break;
            default:
                authData = new byte[0];
        }

        writeBuffer.writeInt((int) clientCapabilities)
                .writeInt(1024 * 1024 * 1024)
                .writeByte(serverLanguage); //1

        writeBuffer.writeBytes((byte) 0, 19)    //19
                .writeInt((int) (clientCapabilities >> 32)); //Maria extended flag

        if (username == null || "".equals(username)) username = System.getProperty("user.name"); //permit SSO
        
        writeBuffer.writeString(username)     //strlen username
                .writeByte((byte) 0);        //1

        if ((clientCapabilities & MariaDbServerCapabilities.PLUGIN_AUTH_LENENC_CLIENT_DATA) != 0) {
            writeBuffer.writeFieldLength(authData.length)
                    .writeByteArray(authData);
        } else if ((clientCapabilities & MariaDbServerCapabilities.SECURE_CONNECTION) != 0) {
            writeBuffer.writeByte((byte) authData.length)
                    .writeByteArray(authData);
        } else {
            writeBuffer.writeByteArray(authData).writeByte((byte) 0);
        }

        if ((clientCapabilities & MariaDbServerCapabilities.CONNECT_WITH_DB) != 0) {
            writeBuffer.writeString(database).writeByte((byte) 0);
        }

        if ((clientCapabilities & MariaDbServerCapabilities.PLUGIN_AUTH) != 0) {
            writeBuffer.writeString(plugin).writeByte((byte) 0);
        }

        if ((clientCapabilities & MariaDbServerCapabilities.CONNECT_ATTRS) != 0) {
            writeConnectAttributes(writeBuffer);
        }
        writeBuffer.finishPacketWithoutRelease(false);
        writeBuffer.releaseBuffer();

    }

    private void writeConnectAttributes(PacketOutputStream writeBuffer) {
        connectionAttributesArray = new byte[200];
        connectionAttributesPosition = 0;
        writeStringLength("_client_name", MariaDbDatabaseMetaData.DRIVER_NAME);
        writeStringLength("_client_version", Version.version);
        writeStringLength("_os", System.getProperty("os.name"));

        String pid = PidFactory.getInstance().getPid();
        if (pid != null) writeStringLength("_pid", pid);

        writeStringLength("_thread", Long.toString(Thread.currentThread().getId()));
        writeStringLength("_java_vendor", System.getProperty("java.vendor"));
        writeStringLength("_java_version", System.getProperty("java.version"));

        if (connectionAttributes != null) {
            StringTokenizer tokenizer = new StringTokenizer(connectionAttributes, ",");
            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken();
                int separator = token.indexOf(":");
                if (separator != -1) {
                    writeStringLength(token.substring(0, separator), token.substring(separator + 1));
                } else {
                    writeStringLength(token, "");
                }
            }
        }
        writeBuffer.writeFieldLength(connectionAttributesPosition);
        writeBuffer.writeByteArray(Arrays.copyOfRange(connectionAttributesArray, 0, connectionAttributesPosition));
    }

    private void writeStringLength(String strKey, String strValue) {
        try {
            final byte[] strBytesKey = strKey.getBytes("UTF-8");
            final byte[] strBytesValue = strValue.getBytes("UTF-8");

            assureBufferCapacity(strBytesKey.length + strBytesValue.length + 18);
            writeFieldLength(strBytesKey.length);
            writeBytes(strBytesKey);

            writeFieldLength(strBytesValue.length);
            writeBytes(strBytesValue);

        } catch (UnsupportedEncodingException u) {
        }
    }

    private void assureBufferCapacity(int additionalSize) {
        if (connectionAttributesArray.length < connectionAttributesPosition + additionalSize) {
            byte[] newConnectionAttributesArray = new byte[Math.max(connectionAttributesArray.length * 2,
                    connectionAttributesPosition + additionalSize)];
            System.arraycopy(connectionAttributesArray, 0, newConnectionAttributesArray, 0, connectionAttributesPosition);
            connectionAttributesArray = newConnectionAttributesArray;
        }
    }

    private void writeFieldLength(long length) {
        if (length < 251) {
            connectionAttributesArray[connectionAttributesPosition++] = (byte) length;
        } else {
            connectionAttributesArray[connectionAttributesPosition++] = (byte) 0xfc;
            connectionAttributesArray[connectionAttributesPosition++] = (byte) (length & 0xff);
            connectionAttributesArray[connectionAttributesPosition++] = (byte) (length >>> 8);
        }
    }

    private void writeBytes(byte[] byteValue) {
        assureBufferCapacity(byteValue.length);
        System.arraycopy(byteValue, 0, this.connectionAttributesArray, connectionAttributesPosition, byteValue.length);
        this.connectionAttributesPosition += byteValue.length;
    }

}
