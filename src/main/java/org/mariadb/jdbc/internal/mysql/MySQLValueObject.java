/*
MariaDB Client for Java

Copyright (c) 2012 Monty Program Ab.

This library is free software; you can redistribute it and/or modify it under
the terms of the GNU Lesser General Public License as published by the Free
Software Foundation; either version 2.1 of the License, or (at your option)
any later version.

This library is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
for more details.

You should have received a copy of the GNU Lesser General Public License along
with this library; if not, write to Monty Program Ab info@montyprogram.com.

This particular MariaDB Client for Java file is work
derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
the following copyright and notice provisions:

Copyright (c) 2009-2011, Marcus Eriksson

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:
Redistributions of source code must retain the above copyright notice, this list
of conditions and the following disclaimer.

Redistributions in binary form must reproduce the above copyright notice, this
list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

Neither the name of the driver nor the names of its contributors may not be
used to endorse or promote products derived from this software without specific
prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
OF SUCH DAMAGE.
*/

package org.mariadb.jdbc.internal.mysql;

import org.mariadb.jdbc.internal.common.AbstractValueObject;

import java.text.ParseException;
import java.util.Calendar;

/**
 * Contains the raw value returned from the server
 *
 * Is immutable
 *
 */
public class MySQLValueObject extends AbstractValueObject {
    MySQLColumnInformation columnInfo;

    public MySQLValueObject(byte[] rawBytes, MySQLColumnInformation columnInfo) {
        super(rawBytes, columnInfo.getType());
        this.columnInfo = columnInfo;
    }

    public String getString() {
        byte[] bytes = getBytes();
        if (bytes == null)
            return null;
        if (columnInfo.getType() == MySQLType.BIT && columnInfo.getLength() == 1)
            return (bytes[0] == 0)?"0":"1";

        return super.getString();
    }

    public Object getObject(int datatypeMappingFlags, Calendar cal) throws ParseException {
        if (this.getBytes() == null) {
            return null;
        }
        switch (dataType) {
        case BIT:
            if (columnInfo.getLength() == 1) {
                return (getBytes()[0] != 0);
            }
            return getBytes();
        case TINYINT:
            if ((datatypeMappingFlags & TINYINT1_IS_BIT) != 0) {
                if (columnInfo.getLength() == 1) {
                    return (getBytes()[0] != '0');
                }
            }
            return getInt();
        case INTEGER:
            if (!columnInfo.isSigned()) {
                return getLong();
            }
            return getInt();
        case BIGINT:
            if (!columnInfo.isSigned()) {
                return getBigInteger();
            }
            return getLong();
        case DOUBLE:
            return getDouble();
        case TIMESTAMP:
            return getTimestamp(cal);
        case DATETIME:
            return getTimestamp(cal);
        case DATE:
            return getDate(cal);
        case VARCHAR:
            if (columnInfo.isBinary())
                return getBytes();
            return getString();
        case DECIMAL:
            return getBigDecimal();
        case BLOB:
            return getBytes();
        case LONGBLOB:
            return getBytes();
        case MEDIUMBLOB:
            return getBytes();
        case TINYBLOB:
            return getBytes();

        case NULL:
            return null;

        case YEAR:
            if ((datatypeMappingFlags & YEAR_IS_DATE_TYPE) != 0) {
                return getDate(cal);
            }
            return getShort();
        case SMALLINT:
        case MEDIUMINT:
            return getInt();
        case FLOAT:
            return getFloat();
        case TIME:
            return getTime(cal);
        case VARSTRING:
        case STRING:
            if (columnInfo.isBinary())
                return getBytes();
            return getString();
        case OLDDECIMAL:
            return getString();
        case GEOMETRY:
            return getBytes();
        case ENUM:
            break;
        case NEWDATE:
            break;
        case SET:
            break;
        default:
            break;
        }
        throw new RuntimeException(dataType.toString());
    }
}
