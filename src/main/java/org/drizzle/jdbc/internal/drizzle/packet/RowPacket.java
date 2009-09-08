/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc.internal.drizzle.packet;

import org.drizzle.jdbc.internal.common.ColumnInformation;
import org.drizzle.jdbc.internal.common.ValueObject;
import org.drizzle.jdbc.internal.common.packet.RawPacket;
import org.drizzle.jdbc.internal.common.packet.buffer.LengthEncodedBytes;
import org.drizzle.jdbc.internal.common.packet.buffer.ReadUtil;
import org.drizzle.jdbc.internal.drizzle.DrizzleType;
import org.drizzle.jdbc.internal.drizzle.DrizzleValueObject;

import java.util.ArrayList;
import java.util.List;

/**
 * User: marcuse Date: Jan 23, 2009 Time: 9:28:43 PM
 */
public class RowPacket {
    private final List<ValueObject> columns;

    public RowPacket(final RawPacket rawPacket, final List<ColumnInformation> columnInformation) {
        columns = new ArrayList<ValueObject>(columnInformation.size());
        final byte[] rawBytes = rawPacket.getRawBytes();
        int readBytes = 0;
        for (final ColumnInformation currentColumn : columnInformation) {
            final LengthEncodedBytes leb = ReadUtil.getLengthEncodedBytes(rawBytes, readBytes);
            readBytes += leb.getLength();
            final DrizzleValueObject dvo = new DrizzleValueObject(leb.getBytes(), (DrizzleType) currentColumn.getType());
            columns.add(dvo);
            currentColumn.updateDisplaySize(dvo.getDisplayLength());
        }
    }

    public List<ValueObject> getRow() {
        return columns;
    }

}
