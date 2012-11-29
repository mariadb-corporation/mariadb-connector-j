/*
MariaDB Client for Java

Copyright (c) 2012 Monty Program Ab. All Rights Reserved.

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

All rights reserved.

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

package org.mariadb.jdbc.internal.mysql.packet.commands;

import org.mariadb.jdbc.internal.common.Utils;
import org.mariadb.jdbc.internal.common.packet.CommandPacket;
import org.mariadb.jdbc.internal.common.packet.PacketOutputStream;
import org.mariadb.jdbc.internal.common.packet.buffer.WriteBuffer;
import org.mariadb.jdbc.internal.mysql.MySQLServerCapabilities;

import java.io.IOException;
import java.io.OutputStream;
import java.security.NoSuchAlgorithmException;
import java.util.Set;

/**
 * 4                            client_flags 4                            max_packet_size 1 charset_number 23 (filler)
 * always 0x00... n (Null-Terminated String)   user n (Length Coded Binary)      scramble_buff (1 + x bytes) 1 (filler)
 * always 0x00 n (Null-Terminated String) databasename
 * <p/>
 * client_flags:            CLIENT_xxx options. The list of possible flag values is in the description of the Handshake
 * Initialisation Packet, for server_capabilities. For some of the bits, the server passed "what it's capable of". The
 * client leaves some of the bits on, adds others, and passes back to the server. One important flag is: whether
 * compression is desired.
 * <p/>
 * max_packet_size:         the maximum number of bytes in a packet for the client
 * <p/>
 * charset_number:          in the same domain as the server_language field that the server passes in the Handshake
 * Initialization packet.
 * <p/>
 * user:                    identification
 * <p/>
 * scramble_buff:           the password, after encrypting using the scramble_buff contents passed by the server (see
 * "Password functions" section elsewhere in this document) if length is zero, no password was given
 * <p/>
 * databasename:            name of schema to use initially
 * <p/>
 */
public class MySQLClientAuthPacket implements CommandPacket {
    private final WriteBuffer writeBuffer;
    private final byte packetSeq;

    public MySQLClientAuthPacket(final String username,
                                 final String password,
                                 final String database,
                                 final Set<MySQLServerCapabilities> serverCapabilities,
                                 final byte[] seed, byte packetSeq) {
        this.packetSeq = packetSeq;
        writeBuffer = new WriteBuffer();
        final byte[] scrambledPassword;
        try {
            scrambledPassword = Utils.encryptPassword(password, seed);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Could not use SHA-1, failing", e);
        }

        final byte serverLanguage = 33;
        writeBuffer.writeInt(MySQLServerCapabilities.fromSet(serverCapabilities)).
                writeInt(1024*1024*1024).
                writeByte(serverLanguage). //1
                writeBytes((byte) 0, 23).    //23
                writeString(username).     //strlen username
                writeByte((byte) 0).        //1
                writeByte((byte) scrambledPassword.length).
                writeByteArray(scrambledPassword); //scrambledPassword.length

        if (serverCapabilities.contains(MySQLServerCapabilities.CONNECT_WITH_DB)) {
            writeBuffer.writeString(database).writeByte((byte) 0);
        }
    }


    public int send(final OutputStream os) throws IOException {
        PacketOutputStream pos = (PacketOutputStream)os;
        pos.startPacket(packetSeq);
        os.write(writeBuffer.getBuffer(),0,writeBuffer.getLength());
        pos.finishPacket();
        return 1;
    }
}