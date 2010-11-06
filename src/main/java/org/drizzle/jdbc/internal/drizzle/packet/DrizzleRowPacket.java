/*
 * Drizzle JDBC
 *
 *  Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 *  All rights reserved.
 *
 *  Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc.internal.drizzle.packet;

import org.drizzle.jdbc.internal.common.ColumnInformation;
import org.drizzle.jdbc.internal.common.ValueObject;
import org.drizzle.jdbc.internal.common.packet.RawPacket;
import org.drizzle.jdbc.internal.common.packet.buffer.Reader;
import org.drizzle.jdbc.internal.drizzle.DrizzleValueObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * User: marcuse Date: Jan 23, 2009 Time: 9:28:43 PM
 */
public class DrizzleRowPacket {
    private final List<ValueObject> columns;

    public DrizzleRowPacket(final RawPacket rawPacket, final List<ColumnInformation> columnInformation) throws IOException {
        columns = new ArrayList<ValueObject>(columnInformation.size());
        final Reader reader = new Reader(rawPacket);
        int readBytes = 0;
        for (final ColumnInformation currentColumn : columnInformation) {
            final ValueObject dvo = new DrizzleValueObject(reader.getLengthEncodedBytes(), currentColumn.getType());
            columns.add(dvo);
            currentColumn.updateDisplaySize(dvo.getDisplayLength());
        }
    }

    public List<ValueObject> getRow() {
        return columns;
    }

}