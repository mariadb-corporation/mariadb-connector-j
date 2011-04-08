/*
 * Drizzle-JDBC
 *
 * Copyright (c) 2009-2011, Marcus Eriksson, Stephane Giron
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following
 * conditions are met:
 *
 *  Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
 *  Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following
 *   disclaimer in the documentation and/or other materials provided with the distribution.
 *  Neither the name of the driver nor the names of its contributors may be used to endorse or promote products derived
 *   from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING,
 * BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
 * EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
 * TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.drizzle.jdbc.internal.common.packet.commands;

import static org.drizzle.jdbc.internal.common.packet.buffer.WriteBuffer.intToByteArray;

import java.io.IOException;
import java.io.OutputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.drizzle.jdbc.internal.common.QueryException;
import org.drizzle.jdbc.internal.common.Utils;
import org.drizzle.jdbc.internal.common.packet.CommandPacket;
import org.drizzle.jdbc.internal.common.query.Query;
import org.drizzle.jdbc.internal.mysql.MySQLProtocol;

/**
 * User: marcuse Date: Jan 19, 2009 Time: 10:14:32 PM
 */
public class StreamedQueryPacket implements CommandPacket
{

    // Maximum packet length coded on 3 bytes
    private static final int MAX_PACKET_LENGTH =  0x00FFFFFF;

    private final static Logger log = Logger
                                            .getLogger(StreamedQueryPacket.class
                                                    .getName());

    private static final int HEADER_LENGTH = 4;

    private final Query         query;

    public StreamedQueryPacket(final Query query)
    {
        this.query = query;

    }

    public int send(final OutputStream ostream) throws IOException,
            QueryException
    {

        if (query.length() > MAX_PACKET_LENGTH - HEADER_LENGTH)
        {
            // Query can not be sent on only one network packet
            return sendSplittedQuery(ostream);
        }
        else
        {
            byte[] byteHeader = Utils.copyWithLength(
                    intToByteArray( query.length() + 1), 5);
            byteHeader[3] = (byte) 0;
            byteHeader[4] = (byte) 0x03;
            log.finest("Sending : " + MySQLProtocol.hexdump(byteHeader, 0));
            ostream.write(byteHeader);

            query.writeTo(ostream);
            ostream.flush();
            return 0;
        }
    }

    private int sendSplittedQuery(OutputStream ostream) throws QueryException,
            IOException
    {
        int remainingBytes = query.length();
        int offset = 0;
        int packetIndex = 0;
        while (remainingBytes >= 0L)
        {
            int packLength = Math.min(remainingBytes, MAX_PACKET_LENGTH);

            byte[] byteHeader = null;
            if (packetIndex == 0)
            {
                byteHeader = Utils.copyWithLength(intToByteArray(packLength), 5);
                // Add the command byte
                byteHeader[4] = (byte) 0x03;
                // And remove 1 byte from available data length
                packLength -= 1;
            }
            else
            {
                byteHeader = Utils.copyWithLength(intToByteArray(packLength), 4);
            }
            byteHeader[3] = (byte) packetIndex;
            if(log.isLoggable(Level.FINEST)) {
                log.finest("Sending packet " + packetIndex + " with length = "
                    + packLength + " / " + remainingBytes);
            }
            ostream.write(byteHeader);
            if(log.isLoggable(Level.FINEST)) {           
                log.finest("Header is " + MySQLProtocol.hexdump(byteHeader, 0));
            }
            if (packLength > 0)
            {
                query.writeTo(ostream, offset, packLength);
            }
            ostream.flush();
            if (remainingBytes >= MAX_PACKET_LENGTH)
            {
                remainingBytes -= packLength;
                offset += packLength;
                packetIndex++;
            }
            else
                remainingBytes = -1;
        }
        return packetIndex;
    }
}