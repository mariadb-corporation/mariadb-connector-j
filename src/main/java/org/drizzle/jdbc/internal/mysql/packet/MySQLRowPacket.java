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
import org.drizzle.jdbc.internal.common.ValueObject;
import org.drizzle.jdbc.internal.common.packet.RawPacket;
import org.drizzle.jdbc.internal.common.packet.buffer.LengthEncodedBytes;
import org.drizzle.jdbc.internal.common.packet.buffer.ReadUtil;
import org.drizzle.jdbc.internal.mysql.MySQLValueObject;

import java.util.ArrayList;
import java.util.List;

/**
 * User: marcuse Date: Jan 23, 2009 Time: 9:28:43 PM
 */
public class MySQLRowPacket {
    private final List<ValueObject> columns;

    public MySQLRowPacket(final RawPacket rawPacket, final List<ColumnInformation> columnInformation) {
        columns = new ArrayList<ValueObject>(columnInformation.size());
        final byte[] rawBytes = rawPacket.getRawBytes();
        int readBytes = 0;
        for (final ColumnInformation currentColumn : columnInformation) {
            final LengthEncodedBytes leb = ReadUtil.getLengthEncodedBytes(rawBytes, readBytes);
            readBytes += leb.getLength();
            final ValueObject dvo = new MySQLValueObject(leb.getBytes(), currentColumn.getType());
            columns.add(dvo);
            currentColumn.updateDisplaySize(dvo.getDisplayLength());
        }
    }

    public List<ValueObject> getRow() {
        return columns;
    }

}