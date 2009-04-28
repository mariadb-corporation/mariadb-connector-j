/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc.internal.drizzle.packet;

import org.drizzle.jdbc.internal.drizzle.packet.buffer.ReadUtil;
import org.drizzle.jdbc.internal.drizzle.packet.buffer.LengthEncodedBytes;
import org.drizzle.jdbc.internal.common.ValueObject;
import org.drizzle.jdbc.internal.common.DrizzleValueObject;
import org.drizzle.jdbc.internal.common.queryresults.ColumnInformation;

import java.util.List;
import java.util.ArrayList;

/**
 * User: marcuse
 * Date: Jan 23, 2009
 * Time: 9:28:43 PM
 */
public class RowPacket {
      private final List<ValueObject> columns;

    public RowPacket(RawPacket rawPacket, List<ColumnInformation> columnInformation) {
        //int fieldCount = columnInformation.size();
        columns = new ArrayList<ValueObject>(columnInformation.size());
        byte [] rawBytes = rawPacket.getRawBytes();
        int readBytes=0;
        for (ColumnInformation currentColumn : columnInformation) {
            LengthEncodedBytes leb = ReadUtil.getLengthEncodedBytes(rawBytes, readBytes);
            readBytes += leb.getLength();

            DrizzleValueObject dvo = new DrizzleValueObject(leb.getBytes(), currentColumn.getType());
            columns.add(dvo);
            currentColumn.updateDisplaySize(dvo.getDisplayLength());
        }
    }

    public List<ValueObject> getRow() {
        return columns;
    }

}
