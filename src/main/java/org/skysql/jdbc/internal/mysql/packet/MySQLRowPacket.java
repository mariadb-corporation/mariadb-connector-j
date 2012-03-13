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

package org.skysql.jdbc.internal.mysql.packet;

import org.skysql.jdbc.internal.common.ColumnInformation;
import org.skysql.jdbc.internal.common.PacketFetcher;
import org.skysql.jdbc.internal.common.ValueObject;
import org.skysql.jdbc.internal.common.packet.RawPacket;
import org.skysql.jdbc.internal.common.packet.buffer.Reader;
import org.skysql.jdbc.internal.mysql.MySQLValueObject;

import java.util.ArrayList;
import java.util.List;
import java.io.IOException;


public class MySQLRowPacket {
    private final List<ValueObject> columns;
    private final Reader reader;
    private final List<ColumnInformation> columnInformation;

    public MySQLRowPacket(final RawPacket rawPacket, final List<ColumnInformation> columnInformation) throws IOException {
        columns = new ArrayList<ValueObject>(columnInformation.size());
        reader = new Reader(rawPacket);
        this.columnInformation = columnInformation;
    }

    public boolean isPacketComplete() throws IOException {

        long encLength = reader.getSilentLengthEncodedBinary();
        long remaining = reader.getRemainingSize();
        return encLength<=remaining;
    }

    public void appendPacket(RawPacket rawPacket) {
        reader.appendPacket(rawPacket);
    }

    public List<ValueObject> getRow(PacketFetcher packetFetcher) throws IOException {
        for (final ColumnInformation currentColumn : columnInformation) {
            while(!isPacketComplete()) {
                appendPacket(packetFetcher.getRawPacket());
            }
            final ValueObject dvo = new MySQLValueObject(reader.getLengthEncodedBytes(), currentColumn);
            columns.add(dvo);
            currentColumn.updateDisplaySize(dvo.getDisplayLength());
        }
        return columns;
    }

}