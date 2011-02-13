/*
 * Drizzle JDBC
 *
 *  Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 *  All rights reserved.
 *
 *  Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc.internal.mysql.packet;

import org.drizzle.jdbc.internal.common.ColumnInformation;
import org.drizzle.jdbc.internal.common.PacketFetcher;
import org.drizzle.jdbc.internal.common.ValueObject;
import org.drizzle.jdbc.internal.common.packet.RawPacket;
import org.drizzle.jdbc.internal.common.packet.buffer.Reader;
import org.drizzle.jdbc.internal.mysql.MySQLValueObject;

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
            if(!isPacketComplete()) {
                appendPacket(packetFetcher.getRawPacket());
            }
            final ValueObject dvo = new MySQLValueObject(reader.getLengthEncodedBytes(), currentColumn.getType());
            columns.add(dvo);
            currentColumn.updateDisplaySize(dvo.getDisplayLength());
        }
        return columns;
    }

}