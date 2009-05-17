/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc;

import org.drizzle.jdbc.internal.common.queryresults.ColumnInformation;
import org.drizzle.jdbc.internal.common.queryresults.ColumnFlags;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.logging.Logger;


/**
 * TODO: finish implem.

 * User: marcuse
 * Date: Feb 8, 2009
 * Time: 9:48:12 PM

 */
public class DrizzleResultSetMetaData implements ResultSetMetaData {
    private final static Logger log = Logger.getLogger(DrizzleResultSetMetaData.class.toString());
    private final List<ColumnInformation> fieldPackets;

    public DrizzleResultSetMetaData(List<ColumnInformation> fieldPackets) {
        this.fieldPackets = fieldPackets;
    }
    /**
     * Returns the number of columns in this <code>ResultSet</code> object.
     *
     * @return the number of columns
     * @throws java.sql.SQLException if a database access error occurs
     */
    public int getColumnCount() throws SQLException {
        return fieldPackets.size();
    }

    /**
     * Indicates whether the designated column is automatically numbered.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @throws java.sql.SQLException if a database access error occurs
     */
    public boolean isAutoIncrement(int column) throws SQLException {
        log.finest("Getting is auto inc");
        return getColumnInformation(column).getFlags().contains(ColumnFlags.AUTO_INCREMENT);
    }

    /**
     * Indicates whether a column's case matters.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @throws java.sql.SQLException if a database access error occurs
     */
    public boolean isCaseSensitive(int column) throws SQLException {
        return getColumnInformation(column).getFlags().contains(ColumnFlags.BINARY); //TODO: of course not this simple
    }

    /**
     * Indicates whether the designated column can be used in a where clause.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @throws java.sql.SQLException if a database access error occurs
     */
    public boolean isSearchable(int column) throws SQLException {

        return true;
    }

    /**
     * Indicates whether the designated column is a cash value.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @throws java.sql.SQLException if a database access error occurs
     */
    public boolean isCurrency(int column) throws SQLException {
        return false;  // no currency columns in drizzle
    }

    /**
     * Indicates the nullability of values in the designated column.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return the nullability status of the given column; one of <code>columnNoNulls</code>,
     *         <code>columnNullable</code> or <code>columnNullableUnknown</code>
     * @throws java.sql.SQLException if a database access error occurs
     */
    public int isNullable(int column) throws SQLException {
        if(getColumnInformation(column).getFlags().contains(ColumnFlags.NOT_NULL))
            return ResultSetMetaData.columnNoNulls;
        else
            return ResultSetMetaData.columnNullable;
    }

    /**
     * Indicates whether values in the designated column are signed numbers.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @throws java.sql.SQLException if a database access error occurs
     */
    public boolean isSigned(int column) throws SQLException {
        return true; //only signed numbers in drizzle
    }

    /**
     * Indicates the designated column's normal maximum width in characters.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return the normal maximum number of characters allowed as the width
     *         of the designated column
     * @throws java.sql.SQLException if a database access error occurs
     */
    public int getColumnDisplaySize(int column) throws SQLException {
        //return getColumnInformation(column).getDisplaySize();
        return 0;
    }

    /**
     * Gets the designated column's suggested title for use in printouts and
     * displays. The suggested title is usually specified by the SQL <code>AS</code>
     * clause.  If a SQL <code>AS</code> is not specified, the value returned from
     * <code>getColumnLabel</code> will be the same as the value returned by the
     * <code>getColumnName</code> method.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return the suggested column title
     * @throws java.sql.SQLException if a database access error occurs
     */
    public String getColumnLabel(int column) throws SQLException {
        return getColumnInformation(column).getName();
    }

    /**
     * Get the designated column's name.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return column name
     * @throws java.sql.SQLException if a database access error occurs
     */
    public String getColumnName(int column) throws SQLException {
        return getColumnInformation(column).getOriginalName();
    }

    /**
     * Get the designated column's table's schema.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return schema name or "" if not applicable
     * @throws java.sql.SQLException if a database access error occurs
     */
    public String getSchemaName(int column) throws SQLException {
        return getColumnInformation(column).getDb();
    }

    /**
     * Get the designated column's specified column size.
     * For numeric data, this is the maximum precision.  For character data, this is the length in characters.
     * For datetime datatypes, this is the length in characters of the String representation (assuming the
     * maximum allowed precision of the fractional seconds component). For binary data, this is the length in bytes.  For the ROWID datatype,
     * this is the length in bytes. 0 is returned for data types where the
     * column size is not applicable.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return precision
     * @throws java.sql.SQLException if a database access error occurs
     */
    public int getPrecision(int column) throws SQLException {
        return 0;
    }

    /**
     * Gets the designated column's number of digits to right of the decimal point.
     * 0 is returned for data types where the scale is not applicable.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return scale
     * @throws java.sql.SQLException if a database access error occurs
     */
    public int getScale(int column) throws SQLException {
        return getColumnInformation(column).getDecimals();
    }

    /**
     * Gets the designated column's table name.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return table name or "" if not applicable
     * @throws java.sql.SQLException if a database access error occurs
     */
    public String getTableName(int column) throws SQLException {
        return getColumnInformation(column).getTable();
    }

    /**
     * Gets the designated column's table's catalog name.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return the name of the catalog for the table in which the given column
     *         appears or "" if not applicable
     * @throws java.sql.SQLException if a database access error occurs
     */
    public String getCatalogName(int column) throws SQLException {
        return "";
    }

    /**
     * Retrieves the designated column's SQL type.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return SQL type from java.sql.Types
     * @throws java.sql.SQLException if a database access error occurs
     * @see java.sql.Types
     */
    public int getColumnType(int column) throws SQLException {
        return getColumnInformation(column).getType().getSqlType();
    }

    /**
     * Retrieves the designated column's database-specific type name.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return type name used by the database. If the column type is
     *         a user-defined type, then a fully-qualified type name is returned.
     * @throws java.sql.SQLException if a database access error occurs
     */
    public String getColumnTypeName(int column) throws SQLException {
        return getColumnInformation(column).getType().toString();
    }

    /**
     * Indicates whether the designated column is definitely not writable.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @throws java.sql.SQLException if a database access error occurs
     */
    public boolean isReadOnly(int column) throws SQLException {
        return false; 
    }

    /**
     * Indicates whether it is possible for a write on the designated column to succeed.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @throws java.sql.SQLException if a database access error occurs
     */
    public boolean isWritable(int column) throws SQLException {
        return !isReadOnly(column);
    }

    /**
     * Indicates whether a write on the designated column will definitely succeed.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @throws java.sql.SQLException if a database access error occurs
     */
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return !isReadOnly(column);
    }

    /**
     * <p>Returns the fully-qualified name of the Java class whose instances
     * are manufactured if the method <code>ResultSet.getObject</code>
     * is called to retrieve a value
     * from the column.  <code>ResultSet.getObject</code> may return a subclass of the
     * class returned by this method.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return the fully-qualified name of the class in the Java programming
     *         language that would be used by the method
     *         <code>ResultSet.getObject</code> to retrieve the value in the specified
     *         column. This is the class name used for custom mapping.
     * @throws java.sql.SQLException if a database access error occurs
     * @since 1.2
     */
    public String getColumnClassName(int column) throws SQLException {
        return getColumnInformation(column).getType().getJavaClass().getName();
    }

    private ColumnInformation getColumnInformation(int column) throws SQLException {
        if(column-1>=0 && column-1<=fieldPackets.size())
            return fieldPackets.get(column-1);
        throw new SQLException("No such column");
    }

    /**
     * Returns an object that implements the given interface to allow access to
     * non-standard methods, or standard methods not exposed by the proxy.
     * <p/>
     * If the receiver implements the interface then the result is the receiver
     * or a proxy for the receiver. If the receiver is a wrapper
     * and the wrapped object implements the interface then the result is the
     * wrapped object or a proxy for the wrapped object. Otherwise return the
     * the result of calling <code>unwrap</code> recursively on the wrapped object
     * or a proxy for that result. If the receiver is not a
     * wrapper and does not implement the interface, then an <code>SQLException</code> is thrown.
     *
     * @param iface A Class defining an interface that the result must implement.
     * @return an object that implements the interface. May be a proxy for the actual implementing object.
     * @throws java.sql.SQLException If no object found that implements the interface
     * @since 1.6
     */
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    /**
     * Returns true if this either implements the interface argument or is directly or indirectly a wrapper
     * for an object that does. Returns false otherwise. If this implements the interface then return true,
     * else if this is a wrapper then return the result of recursively calling <code>isWrapperFor</code> on the wrapped
     * object. If this does not implement the interface and is not a wrapper, return false.
     * This method should be implemented as a low-cost operation compared to <code>unwrap</code> so that
     * callers can use this method to avoid expensive <code>unwrap</code> calls that may fail. If this method
     * returns true then calling <code>unwrap</code> with the same argument should succeed.
     *
     * @param iface a Class defining an interface.
     * @return true if this implements the interface or directly or indirectly wraps an object that does.
     * @throws java.sql.SQLException if an error occurs while determining whether this is a wrapper
     *                               for an object with the given interface.
     * @since 1.6
     */
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}
