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

package org.skysql.jdbc.internal.common.packet;

import org.skysql.jdbc.internal.common.packet.buffer.Reader;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Set;

/**
 * . User: marcuse Date: Jan 16, 2009 Time: 4:23:54 PM
 */
public class EOFPacket extends ResultPacket {
    public enum ServerStatus {
        SERVER_STATUS_IN_TRANS(1),
        SERVER_STATUS_AUTOCOMMIT(2),
        SERVER_MORE_RESULTS_EXISTS(8),
        SERVER_QUERY_NO_GOOD_INDEX_USED(16),
        SERVER_QUERY_NO_INDEX_USED(32),
        SERVER_STATUS_DB_DROPPED(256);
        private final int bitmapFlag;
        ServerStatus(int bitmapFlag) {
            this.bitmapFlag = bitmapFlag;
        }
        public static Set<ServerStatus> getServerCapabilitiesSet(final short i) {
            final Set<ServerStatus> statusSet = EnumSet.noneOf(ServerStatus.class);
            for (ServerStatus value : ServerStatus.values()) {
                if ((i & value.getBitmapFlag()) == value.getBitmapFlag()) {
                    statusSet.add(value);
                }
            }
            return statusSet;
        }


        public int getBitmapFlag() {
            return bitmapFlag;
        }
    }

    private final byte packetSeq;
    private final short warningCount;
    private final Set<ServerStatus> statusFlags;


    public EOFPacket(final RawPacket rawPacket) throws IOException {
        final Reader reader = new Reader(rawPacket);
        packetSeq = 0;
        reader.readByte();
        warningCount = reader.readShort();
        statusFlags = ServerStatus.getServerCapabilitiesSet(reader.readShort());
    }

    public ResultType getResultType() {
        return ResultType.EOF;
    }

    public byte getPacketSeq() {
        return packetSeq;
    }

    public short getWarningCount() {
        return warningCount;
    }

    public Set<ServerStatus> getStatusFlags() {
        return statusFlags;
    }

    @Override
    public String toString() {
        return "EOFPacket{" +
                "packetSeq=" + packetSeq +
                ", warningCount=" + warningCount +
                ", statusFlags=" + statusFlags +
                '}';
    }
}
