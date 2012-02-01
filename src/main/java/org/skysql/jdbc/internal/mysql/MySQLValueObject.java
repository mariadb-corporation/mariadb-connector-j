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

package org.skysql.jdbc.internal.mysql;

import org.skysql.jdbc.internal.common.AbstractValueObject;
import org.skysql.jdbc.internal.common.ColumnInformation;

import java.text.ParseException;

/**
 * Contains the raw value returned from the server
 * <p/>
 * Is immutable
 * <p/>
 * User: marcuse Date: Feb 16, 2009 Time: 9:18:26 PM
 */
public class MySQLValueObject extends AbstractValueObject {
    ColumnInformation columnInfo;

    public MySQLValueObject(byte[] rawBytes, ColumnInformation columnInfo) {
        super(rawBytes, columnInfo.getType());
        this.columnInfo = columnInfo;
    }

    public Object getObject(int datatypeMappingFlags) throws ParseException {
        if (this.getBytes() == null) {
            return null;
        }
        switch (dataType.getType()) {
            case BIT:
                if (columnInfo.getLength() == 1) {
                    return getInt() == 1;
                }
                return getBytes();
            case TINYINT:
                if ((datatypeMappingFlags & TINYINT1_IS_BIT) != 0) {
                    if (columnInfo.getLength() == 1) {
                        return getInt() == 1;
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
                return getTimestamp();
            case DATETIME:
                return getTimestamp();
            case DATE:
                return getDate();
            case VARCHAR:
                if (columnInfo.isBinary())
                    return getBytes();
                return getString();
            case DECIMAL:
                return getBigDecimal();
            case BLOB:
                return getBytes();
            case YEAR:
                if ((datatypeMappingFlags & YEAR_IS_DATE_TYPE) != 0) {
                    return getDate();
                }
                return getShort();
            case SMALLINT:
            case MEDIUMINT:
                return getInt();
            case FLOAT:
                return getFloat();
            case TIME:
                return getTime();
            case CLOB:
                return getString();
            case CHAR:
                return getString();
        }
        return null;
    }
}
