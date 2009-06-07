/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc.internal.mysql.packet.commands;

import org.drizzle.jdbc.internal.common.Utils;
import org.drizzle.jdbc.internal.common.packet.CommandPacket;
import org.drizzle.jdbc.internal.common.packet.buffer.WriteBuffer;
import org.drizzle.jdbc.internal.mysql.MySQLServerCapabilities;

import java.io.IOException;
import java.io.OutputStream;
import java.security.NoSuchAlgorithmException;
import java.util.Set;

/**
 * 4                            client_flags
 * 4                            max_packet_size
 * 1                            charset_number
 * 23                           (filler) always 0x00...
 * n (Null-Terminated String)   user
 * n (Length Coded Binary)      scramble_buff (1 + x bytes)
 * 1                            (filler) always 0x00
 * n (Null-Terminated String)   databasename
 * <p/>
 * client_flags:            CLIENT_xxx options. The list of possible flag
 * values is in the description of the Handshake
 * Initialisation Packet, for server_capabilities.
 * For some of the bits, the server passed "what
 * it's capable of". The client leaves some of the
 * bits on, adds others, and passes back to the server.
 * One important flag is: whether compression is desired.
 * <p/>
 * max_packet_size:         the maximum number of bytes in a packet for the client
 * <p/>
 * charset_number:          in the same domain as the server_language field that
 * the server passes in the Handshake Initialization packet.
 * <p/>
 * user:                    identification
 * <p/>
 * scramble_buff:           the password, after encrypting using the scramble_buff
 * contents passed by the server (see "Password functions"
 * section elsewhere in this document)
 * if length is zero, no password was given
 * <p/>
 * databasename:            name of schema to use initially
 * <p/>
 * User: marcuse
 * Date: Jan 16, 2009
 * Time: 11:19:31 AM
 */
public class MySQLClientAuthPacket implements CommandPacket {
    private final WriteBuffer writeBuffer;
    private final Set<MySQLServerCapabilities> serverCapabilities;
    private final String username;
    private final String password;
    private final String database;

    public MySQLClientAuthPacket(String username, String password, String database, Set<MySQLServerCapabilities> serverCapabilities, byte[] seed) {
        writeBuffer = new WriteBuffer();
        this.username = username;
        this.password = password;
        this.database = database;
        this.serverCapabilities = serverCapabilities;
        byte[] scrambledPassword;
        try {
            scrambledPassword = Utils.encryptPassword(password, seed);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Could not use SHA-1, failing", e);
        }
        int packetLength = 4 + 4 + 1 + 23 + username.length() + 1 + scrambledPassword.length + 1 + database.length() + 1;
        byte serverLanguage = 45;
        writeBuffer.writeInt(MySQLServerCapabilities.fromSet(serverCapabilities)).
                writeInt(packetLength).
                writeByte(serverLanguage). //1
                writeBytes((byte) 0, 23).    //23
                writeString(username).     //strlen username
                writeByte((byte) 0).        //1
                writeByte((byte) scrambledPassword.length).
                writeByteArray(scrambledPassword). //scrambledPassword.length
                writeString(database).     //strlen(database)
                writeByte((byte) 0);
    }


    public void send(OutputStream os) throws IOException {
        byte[] buff = writeBuffer.toByteArrayWithLength((byte) 1);
        for (byte b : buff) {
            os.write(b);
        }
        os.flush();
    }
}