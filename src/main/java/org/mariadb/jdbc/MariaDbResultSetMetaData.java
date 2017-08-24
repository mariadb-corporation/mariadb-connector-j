/*
 *
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2017 MariaDB Ab.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 * This particular MariaDB Client for Java file is work
 * derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
 * the following copyright and notice provisions:
 *
 * Copyright (c) 2009-2011, Marcus Eriksson
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 * Redistributions of source code must retain the above copyright notice, this list
 * of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice, this
 * list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * Neither the name of the driver nor the names of its contributors may not be
 * used to endorse or promote products derived from this software without specific
 * prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 */

package org.mariadb.jdbc;

import org.mariadb.jdbc.internal.ColumnType;
import org.mariadb.jdbc.internal.com.read.resultset.ColumnInformation;
import org.mariadb.jdbc.internal.com.read.resultset.SelectResultSet;
import org.mariadb.jdbc.internal.util.constant.ColumnFlags;
import org.mariadb.jdbc.internal.util.exceptions.ExceptionMapper;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;


public class MariaDbResultSetMetaData implements ResultSetMetaData {

    private final ColumnInformation[] fieldPackets;
    private final int datatypeMappingflags;
    private final boolean returnTableAlias;

    /**
     * Constructor.
     *
     * @param fieldPackets         column informations
     * @param datatypeMappingFlags Data type
     * @param returnTableAlias     must return table alias or real table name
     */
    public MariaDbResultSetMetaData(ColumnInformation[] fieldPackets, int datatypeMappingFlags, boolean returnTableAlias) {
        this.fieldPackets = fieldPackets;
        this.datatypeMappingflags = datatypeMappingFlags;
        this.returnTableAlias = returnTableAlias;
    }

    /**
     * Returns the number of columns in this <code>ResultSet</code> object.
     *
     * @return the number of columns
     * @throws SQLException if a database access error occurs
     */
    public int getColumnCount() throws SQLException {
        return fieldPackets.length;
    }

    /**
     * Indicates whether the designated column is automatically numbered.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean isAutoIncrement(final int column) throws SQLException {
        return (getColumnInformation(column).getFlags() & ColumnFlags.AUTO_INCREMENT) != 0;
    }

    /**
     * Indicates whether a column's case matters.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean isCaseSensitive(final int column) throws SQLException {
        return (getColumnInformation(column).getFlags() & ColumnFlags.BINARY_COLLATION) != 0;
    }

    /**
     * Indicates whether the designated column can be used in a where clause.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean isSearchable(final int column) throws SQLException {
        return true;
    }

    /**
     * Indicates whether the designated column is a cash value.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean isCurrency(final int column) throws SQLException {
        return false;
    }

    /**
     * Indicates the nullability of values in the designated column.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return the nullability status of the given column; one of <code>columnNoNulls</code>,
     * <code>columnNullable</code> or <code>columnNullableUnknown</code>
     * @throws SQLException if a database access error occurs
     */
    public int isNullable(final int column) throws SQLException {
        if ((getColumnInformation(column).getFlags() & ColumnFlags.NOT_NULL) == 0) {
            return ResultSetMetaData.columnNullable;
        } else {
            return ResultSetMetaData.columnNoNulls;
        }
    }

    /**
     * Indicates whether values in the designated column are signed numbers.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean isSigned(int column) throws SQLException {
        return getColumnInformation(column).isSigned();
    }

    /**
     * Indicates the designated column's normal maximum width in characters.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return the normal maximum number of characters allowed as the width of the designated column
     * @throws SQLException if a database access error occurs
     */
    public int getColumnDisplaySize(final int column) throws SQLException {
        return getColumnInformation(column).getDisplaySize();
    }

    /**
     * Gets the designated column's suggested title for use in printouts and displays. The suggested title is usually
     * specified by the SQL <code>AS</code> clause.  If a SQL <code>AS</code> is not specified, the value returned from
     * <code>getColumnLabel</code> will be the same as the value returned by the <code>getColumnName</code> method.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return the suggested column title
     * @throws SQLException if a database access error occurs
     */
    public String getColumnLabel(final int column) throws SQLException {
        return getColumnInformation(column).getName();
    }

    /**
     * Get the designated column's name.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return column name
     * @throws SQLException if a database access error occurs
     */
    public String getColumnName(final int column) throws SQLException {
        String columnName = getColumnInformation(column).getOriginalName();
        if (returnTableAlias) {
            columnName = getColumnInformation(column).getName(); //for old mysql compatibility
        }

        if ("".equals(columnName)) {
            // odd things that are no columns, e.g count(*)
            columnName = getColumnLabel(column);
        }
        return columnName;
    }

    /**
     * Get the designated column's table's schema.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return schema name or "" if not applicable
     * @throws SQLException if a database access error occurs
     */
    public String getCatalogName(int column) throws SQLException {
        return getColumnInformation(column).getDb();
    }

    /**
     * Get the designated column's specified column size. For numeric data, this is the maximum precision.  For
     * character data, this is the length in characters. For datetime datatypes, this is the length in characters of the
     * String representation (assuming the maximum allowed precision of the fractional seconds component). For binary
     * data, this is the length in bytes.  For the ROWID datatype, this is the length in bytes. 0 is returned for data
     * types where the column size is not applicable.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return precision
     * @throws SQLException if a database access error occurs
     */
    public int getPrecision(final int column) throws SQLException {
        return (int) getColumnInformation(column).getPrecision();
    }

    /**
     * Gets the designated column's number of digits to right of the decimal point. 0 is returned for data types where
     * the scale is not applicable.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return scale
     * @throws SQLException if a database access error occurs
     */
    public int getScale(final int column) throws SQLException {
        return getColumnInformation(column).getDecimals();
    }

    /**
     * Gets the designated column's table name.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return table name or "" if not applicable
     * @throws SQLException if a database access error occurs
     */
    public String getTableName(final int column) throws SQLException {
        if (returnTableAlias) {
            return getColumnInformation(column).getTable();
        } else {
            return getColumnInformation(column).getOriginalTable();
        }
    }


    public String getSchemaName(int column) throws SQLException {
        return "";
    }

    /**
     * Retrieves the designated column's SQL type.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return SQL type from java.sql.Types
     * @throws SQLException if a database access error occurs
     * @see Types
     */
    public int getColumnType(final int column) throws SQLException {
        ColumnInformation ci = getColumnInformation(column);
        switch (ci.getColumnType()) {
            case BIT:
                if (ci.getLength() == 1) {
                    return Types.BIT;
                }
                return Types.VARBINARY;
            case TINYINT:
                if (ci.getLength() == 1 && (datatypeMappingflags & SelectResultSet.TINYINT1_IS_BIT) != 0) {
                    return Types.BIT;
                } else {
                    return Types.TINYINT;
                }
            case YEAR:
                if ((datatypeMappingflags & SelectResultSet.YEAR_IS_DATE_TYPE) != 0) {
                    return Types.DATE;
                } else {
                    return Types.SMALLINT;
                }
            case BLOB:
                if (ci.getLength() < 0 || ci.getLength() > 16777215) {
                    return Types.LONGVARBINARY;
                }
                return Types.VARBINARY;
            case VARCHAR:
            case VARSTRING:
                if (ci.isBinary()) {
                    return Types.VARBINARY;
                }
                if (ci.getLength() < 0) {
                    return Types.LONGVARCHAR;
                }
                return Types.VARCHAR;
            case STRING:
                if (ci.isBinary()) {
                    return Types.BINARY;
                }
                return Types.CHAR;
            default:
                return ci.getColumnType().getSqlType();
        }

    }

    /**
     * Retrieves the designated column's database-specific type name.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return type name used by the database. If the column type is a user-defined type, then a fully-qualified type
     * name is returned.
     * @throws SQLException if a database access error occurs
     */
    public String getColumnTypeName(final int column) throws SQLException {
        ColumnInformation ci = getColumnInformation(column);
        return ColumnType.getColumnTypeName(ci.getColumnType(), ci.getLength(), ci.isSigned(), ci.isBinary());

    }

    /**
     * Indicates whether the designated column is definitely not writable.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean isReadOnly(final int column) throws SQLException {
        return false;
    }

    /**
     * Indicates whether it is possible for a write on the designated column to succeed.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean isWritable(final int column) throws SQLException {
        return !isReadOnly(column);
    }

    /**
     * Indicates whether a write on the designated column will definitely succeed.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean isDefinitelyWritable(final int column) throws SQLException {
        return !isReadOnly(column);
    }

    /**
     * <p>Returns the fully-qualified name of the Java class whose instances are manufactured if the method
     * <code>ResultSet.getObject</code> is called to retrieve a value from the column.  <code>ResultSet.getObject</code>
     * may return a subclass of the class returned by this method.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return the fully-qualified name of the class in the Java programming language that would be used by the method
     * <code>ResultSet.getObject</code> to retrieve the value in the specified column. This is the class name
     * used for custom mapping.
     * @throws SQLException if a database access error occurs
     * @since 1.2
     */

    public String getColumnClassName(int column) throws SQLException {
        ColumnInformation ci = getColumnInformation(column);
        ColumnType type = ci.getColumnType();
        return ColumnType.getClassName(type, (int) ci.getLength(), ci.isSigned(), ci.isBinary(), datatypeMappingflags);
    }

    private ColumnInformation getColumnInformation(int column) throws SQLException {
        if (column >= 1 && column <= fieldPackets.length) {
            return fieldPackets[column - 1];
        }
        throw ExceptionMapper.getSqlException("No such column");
    }

    /**
     * Returns an object that implements the given interface to allow access to non-standard methods, or standard
     * methods not exposed by the proxy.
     * <br>
     * If the receiver implements the interface then the result is the receiver or a proxy for the receiver. If the
     * receiver is a wrapper and the wrapped object implements the interface then the result is the wrapped object or a
     * proxy for the wrapped object. Otherwise return the the result of calling <code>unwrap</code> recursively on the
     * wrapped object or a proxy for that result. If the receiver is not a wrapper and does not implement the interface,
     * then an <code>SQLException</code> is thrown.
     *
     * @param iface A Class defining an interface that the result must implement.
     * @return an object that implements the interface. May be a proxy for the actual implementing object.
     * @throws SQLException If no object found that implements the interface
     * @since 1.6
     */
    public <T> T unwrap(final Class<T> iface) throws SQLException {
        try {
            if (isWrapperFor(iface)) {
                return iface.cast(this);
            } else {
                throw new SQLException("The receiver is not a wrapper for " + iface.getName());
            }
        } catch (Exception e) {
            throw new SQLException("The receiver is not a wrapper and does not implement the interface");
        }
    }

    /**
     * Returns true if this either implements the interface argument or is directly or indirectly a wrapper for an object that does. Returns false
     * otherwise. If this implements the interface then return true, else if this is a wrapper then return the result of recursively calling
     * <code>isWrapperFor</code> on the wrapped object. If this does not implement the interface and is not a wrapper, return false. This method
     * should be implemented as a low-cost operation compared to <code>unwrap</code> so that callers can use this method to avoid expensive
     * <code>unwrap</code> calls that may fail. If this method returns true then calling <code>unwrap</code> with the same argument should succeed.
     *
     * @param iface a Class defining an interface.
     * @return true if this implements the interface or directly or indirectly wraps an object that does.
     * @throws SQLException if an error occurs while determining whether this is a wrapper for an object with the given interface.
     * @since 1.6
     */
    public boolean isWrapperFor(final Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }
}
