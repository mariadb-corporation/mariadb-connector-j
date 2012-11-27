/*
 * Drizzle-JDBC
 *
 * Copyright (c) 2009-2011, Marcus Eriksson
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

package org.mariadb.jdbc.internal.common.packet;

import org.mariadb.jdbc.internal.common.packet.buffer.Reader;
import org.mariadb.jdbc.internal.mysql.MySQLProtocol;

import java.io.IOException;

/**
 * . User: marcuse Date: Jan 21, 2009 Time: 10:40:03 PM
 */
public class ResultSetPacket extends ResultPacket {
    private final long fieldCount;

    public ResultSetPacket(final RawPacket rawPacket) throws IOException {
        final Reader reader = new Reader(rawPacket);
        
        fieldCount = reader.getLengthEncodedBinary();
        if (fieldCount == -1) {
            // Should never get there, it is LocalInfilePacket, not ResultSetPacket
            throw new AssertionError("field count is -1 in ResultSetPacket.");
        }
        if (reader.getRemainingSize() != 0) {
              throw new IOException("invalid packet contents ,expected result set packet, actual packet hexdump = " +
                    MySQLProtocol.hexdump(rawPacket.getByteBuffer(),0));
        }
    }

    public ResultType getResultType() {
        return ResultPacket.ResultType.RESULTSET;
    }

    public byte getPacketSeq() {
        return 0;
    }

    public long getFieldCount() {
        return fieldCount;
    }
}
