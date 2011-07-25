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

package org.skysql.jdbc.internal.common.packet.commands;

import org.skysql.jdbc.internal.common.QueryException;
import org.skysql.jdbc.internal.common.packet.CommandPacket;
import org.skysql.jdbc.internal.common.packet.PacketOutputStream;
import org.skysql.jdbc.internal.common.query.Query;

import java.io.IOException;
import java.io.OutputStream;
import java.util.logging.Logger;

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
        PacketOutputStream pos = (PacketOutputStream)ostream;
        pos.startPacket(0);
        pos.write(0x03);
        query.writeTo(pos);
        pos.finishPacket();
        return 0;
    }
}