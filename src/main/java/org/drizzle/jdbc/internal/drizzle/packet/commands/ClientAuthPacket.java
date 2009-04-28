/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc.internal.drizzle.packet.commands;

import org.drizzle.jdbc.internal.drizzle.packet.buffer.WriteBuffer;
import org.drizzle.jdbc.internal.drizzle.packet.CommandPacket;
import org.drizzle.jdbc.internal.drizzle.ServerCapabilities;

import java.util.Set;
import java.io.OutputStream;
import java.io.IOException;

/**
 4                            client_flags
 4                            max_packet_size
 1                            charset_number
 23                           (filler) always 0x00...
 n (Null-Terminated String)   user
 n (Length Coded Binary)      scramble_buff (1 + x bytes)
 1                            (filler) always 0x00
 n (Null-Terminated String)   databasename

 client_flags:            CLIENT_xxx options. The list of possible flag
                          values is in the description of the Handshake
                          Initialisation Packet, for server_capabilities.
                          For some of the bits, the server passed "what
                          it's capable of". The client leaves some of the
                          bits on, adds others, and passes back to the server.
                          One important flag is: whether compression is desired.

 max_packet_size:         the maximum number of bytes in a packet for the client

 charset_number:          in the same domain as the server_language field that
                          the server passes in the Handshake Initialization packet.

 user:                    identification

 scramble_buff:           the password, after encrypting using the scramble_buff
                          contents passed by the server (see "Password functions"
                          section elsewhere in this document)
                          if length is zero, no password was given

 databasename:            name of schema to use initially

 * User: marcuse
 * Date: Jan 16, 2009
 * Time: 11:19:31 AM

 */
public class ClientAuthPacket implements CommandPacket {
    private final WriteBuffer writeBuffer;
    private final Set<ServerCapabilities> serverCapabilities;
    private final byte serverLanguage=45;
    private final String username;
    private final String password;
    private final String database;

    public ClientAuthPacket(String username,String password,String database, Set<ServerCapabilities> serverCapabilities) {
        writeBuffer = new WriteBuffer();
        this.username=username;
        this.password=password;
        this.database=database;
        this.serverCapabilities=serverCapabilities;
        writeBuffer.writeInt(ServerCapabilities.fromSet(serverCapabilities)).
                writeInt(4+4+1+23+username.length()+1+1+database.length()+1).
                    writeByte(serverLanguage). //1
                    writeBytes((byte)0,23).    //23
                    writeString(username).     //strlen username
                    writeByte((byte)0).        //1
                    //writeString(scramblePassword(password)) //strlen(scramb)
                    writeByte((byte)0).        //1
                    //writeByte((byte)0).
                    writeString(database).     //strlen(database)
                    writeByte((byte)0);
    }


    public void send(OutputStream os) throws IOException {
        byte [] buff = writeBuffer.toByteArrayWithLength((byte)1);
        for(byte b:buff)
            os.write(b);
        os.flush();
    }
}
