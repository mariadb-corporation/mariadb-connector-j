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

package org.skysql.jdbc;

import org.skysql.jdbc.internal.SQLExceptionMapper;
import org.skysql.jdbc.internal.common.ColumnInformation;
import org.skysql.jdbc.internal.common.Protocol;
import org.skysql.jdbc.internal.common.QueryException;
import org.skysql.jdbc.internal.common.ValueObject;
import org.skysql.jdbc.internal.common.queryresults.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.*;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * . User: marcuse Date: Jan 19, 2009 Time: 10:25:00 PM
 */
public class MySQLResultSet implements ResultSet {

    public final static MySQLResultSet EMPTY = createEmptyResultSet();
    private final QueryResult queryResult;
    private final Statement statement;
    private final Protocol protocol;
    // dont want these, but jdbc forces them with "lastGetWasNull" etc...
    private boolean isClosed;
    private boolean lastGetWasNull;
    private boolean warningsCleared;
    ColumnNameMap columnNameMap;

    public MySQLResultSet(final QueryResult dqr, final Statement statement, final Protocol protocol) {
        this.queryResult = dqr;
        this.statement = statement;
        isClosed = false;
        this.protocol = protocol;
        this.columnNameMap = new ColumnNameMap(dqr.getColumnInformation());
    }

    private static MySQLResultSet createEmptyResultSet() {
        final List<ColumnInformation> colList = Collections.emptyList();
        final List<List<ValueObject>> voList = Collections.emptyList();
        final QueryResult qr = new CachedSelectResult(colList, voList, (short) 0);
        return new MySQLResultSet(qr, null, null);
    }

    public boolean next() throws SQLException {
        try {
            return queryResult.getResultSetType() == ResultSetType.SELECT
                && ((SelectQueryResult) queryResult).next();
        } catch(IOException ioe) {
            throw new SQLException(ioe);
        } catch (QueryException qe) {
            throw new SQLException(qe);
        }
    }

    public void close() throws SQLException {
        this.isClosed = true;
        if (this.queryResult != null) {
            this.queryResult.close();
        }
    }

    /**
     * Reports whether the last column read had a value of SQL <code>NULL</code>. Note that you must first call one of
     * the getter methods on a column to try to read its value and then call the method <code>wasNull</code> to see if
     * the value read was SQL <code>NULL</code>.
     *
     * @return <code>true</code> if the last column value read was SQL <code>NULL</code> and <code>false</code>
     *         otherwise
     * @throws java.sql.SQLException if a database access error occurs or this method is called on a closed result set
     */
    public boolean wasNull() throws SQLException {
        return lastGetWasNull;
    }


    public String getString(final int i) throws SQLException {
        return getValueObject(i).getString();
    }

    public int getInt(final int i) throws SQLException {
        return getValueObject(i).getInt();
    }

    public int getInt(final String s) throws SQLException {
        return getValueObject(s).getInt();
    }

    private ValueObject getValueObject(final int i) throws SQLException {
        if (queryResult.getResultSetType() == ResultSetType.SELECT) {
            final ValueObject vo;
            try {
                vo = ((SelectQueryResult) queryResult).getValueObject(i - 1);
            } catch (NoSuchColumnException e) {
                throw SQLExceptionMapper.getSQLException(e.getMessage(), e);
            }
            this.lastGetWasNull = vo.isNull();
            return vo;
        }
        throw SQLExceptionMapper.getSQLException("Cannot get data from update-result sets");
    }

    private ValueObject getValueObject(final String column) throws SQLException {
        return getValueObject(columnNameMap.getIndex(column)+1);
    }

    /**
     * Retrieves the value of the designated column in the current row of this <code>ResultSet</code> object as a
     * <code>long</code> in the Java programming language.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @return the column value; if the value is SQL <code>NULL</code>, the value returned is <code>0</code>
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs or this method
     *                               is called on a closed result set
     */
    public long getLong(final String columnLabel) throws SQLException {
        return getValueObject(columnLabel).getLong();
    }

    /**
     * Retrieves the value of the designated column in the current row of this <code>ResultSet</code> object as a
     * <code>float</code> in the Java programming language.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @return the column value; if the value is SQL <code>NULL</code>, the value returned is <code>0</code>
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs or this method
     *                               is called on a closed result set
     */
    public float getFloat(final String columnLabel) throws SQLException {
        return getValueObject(columnLabel).getFloat();
    }

    /**
     * Retrieves the value of the designated column in the current row of this <code>ResultSet</code> object as a
     * <code>double</code> in the Java programming language.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @return the column value; if the value is SQL <code>NULL</code>, the value returned is <code>0</code>
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs or this method
     *                               is called on a closed result set
     */
    public double getDouble(final String columnLabel) throws SQLException {
        return getValueObject(columnLabel).getDouble();
    }

    /**
     * Retrieves the value of the designated column in the current row of this <code>ResultSet</code> object as a
     * <code>java.math.BigDecimal</code> in the Java programming language.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @param scale       the number of digits to the right of the decimal point
     * @return the column value; if the value is SQL <code>NULL</code>, the value returned is <code>null</code>
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs or this method
     *                               is called on a closed result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @deprecated
     */
    public BigDecimal getBigDecimal(final String columnLabel, final int scale) throws SQLException {
        return getValueObject(columnLabel).getBigDecimal();
    }

    /**
     * Retrieves the value of the designated column in the current row of this <code>ResultSet</code> object as a
     * <code>byte</code> array in the Java programming language. The bytes represent the raw values returned by the
     * driver.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @return the column value; if the value is SQL <code>NULL</code>, the value returned is <code>null</code>
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs or this method
     *                               is called on a closed result set
     */
    public byte[] getBytes(final String columnLabel) throws SQLException {
        return getValueObject(columnLabel).getBytes();
    }

    /**
     * Retrieves the value of the designated column in the current row of this <code>ResultSet</code> object as a
     * <code>java.sql.Date</code> object in the Java programming language.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @return the column value; if the value is SQL <code>NULL</code>, the value returned is <code>null</code>
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs or this method
     *                               is called on a closed result set
     */
    public Date getDate(final String columnLabel) throws SQLException {
        try {
            return getValueObject(columnLabel).getDate();
        } catch (ParseException e) {
            throw SQLExceptionMapper.getSQLException("Could not parse date", e);
        }
    }

    /**
     * Retrieves the value of the designated column in the current row of this <code>ResultSet</code> object as a
     * <code>java.sql.Time</code> object in the Java programming language.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @return the column value; if the value is SQL <code>NULL</code>, the value returned is <code>null</code>
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs or this method
     *                               is called on a closed result set
     */
    public Time getTime(final String columnLabel) throws SQLException {
        try {
            return getValueObject(columnLabel).getTime();
        } catch (ParseException e) {
            throw SQLExceptionMapper.getSQLException("Could not parse column as time, was: \"" +
                    getValueObject(columnLabel).getString() +
                    "\"", e);
        }
    }

    /**
     * Retrieves the value of the designated column in the current row of this <code>ResultSet</code> object as a
     * <code>java.sql.Timestamp</code> object in the Java programming language.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @return the column value; if the value is SQL <code>NULL</code>, the value returned is <code>null</code>
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs or this method
     *                               is called on a closed result set
     */
    public Timestamp getTimestamp(final String columnLabel) throws SQLException {
        try {
            return getValueObject(columnLabel).getTimestamp();
        } catch (ParseException e) {
            throw SQLExceptionMapper.getSQLException("Could not parse column as timestamp, was: \"" +
                    getValueObject(columnLabel).getString() +
                    "\"", e);
        }
    }

    /**
     * Retrieves the value of the designated column in the current row of this <code>ResultSet</code> object as a stream
     * of ASCII characters. The value can then be read in chunks from the stream. This method is particularly suitable
     * for retrieving large <code>LONGVARCHAR</code> values. The JDBC driver will do any necessary conversion from the
     * database format into ASCII.
     * <p/>
     * <P><B>Note:</B> All the data in the returned stream must be read prior to getting the value of any other column.
     * The next call to a getter method implicitly closes the stream. Also, a stream may return <code>0</code> when the
     * method <code>available</code> is called whether there is data available or not.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @return a Java input stream that delivers the database column value as a stream of one-byte ASCII characters. If
     *         the value is SQL <code>NULL</code>, the value returned is <code>null</code>.
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs or this method
     *                               is called on a closed result set
     */
    public InputStream getAsciiStream(final String columnLabel) throws SQLException {
        return getValueObject(columnLabel).getInputStream();

    }

    /**
     * Retrieves the value of the designated column in the current row of this <code>ResultSet</code> object as a stream
     * of two-byte Unicode characters. The first byte is the high byte; the second byte is the low byte.
     * <p/>
     * The value can then be read in chunks from the stream. This method is particularly suitable for retrieving large
     * <code>LONGVARCHAR</code> values. The JDBC technology-enabled driver will do any necessary conversion from the
     * database format into Unicode.
     * <p/>
     * <P><B>Note:</B> All the data in the returned stream must be read prior to getting the value of any other column.
     * The next call to a getter method implicitly closes the stream. Also, a stream may return <code>0</code> when the
     * method <code>InputStream.available</code> is called, whether there is data available or not.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @return a Java input stream that delivers the database column value as a stream of two-byte Unicode characters.
     *         If the value is SQL <code>NULL</code>, the value returned is <code>null</code>.
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs or this method
     *                               is called on a closed result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @deprecated use <code>getCharacterStream</code> instead
     */
    public InputStream getUnicodeStream(final String columnLabel) throws SQLException {
        return getValueObject(columnLabel).getInputStream();
    }

    /**
     * Retrieves the value of the designated column in the current row of this <code>ResultSet</code> object as a stream
     * of uninterpreted <code>byte</code>s. The value can then be read in chunks from the stream. This method is
     * particularly suitable for retrieving large <code>LONGVARBINARY</code> values.
     * <p/>
     * <P><B>Note:</B> All the data in the returned stream must be read prior to getting the value of any other column.
     * The next call to a getter method implicitly closes the stream. Also, a stream may return <code>0</code> when the
     * method <code>available</code> is called whether there is data available or not.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @return a Java input stream that delivers the database column value as a stream of uninterpreted bytes; if the
     *         value is SQL <code>NULL</code>, the result is <code>null</code>
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs or this method
     *                               is called on a closed result set
     */
    public InputStream getBinaryStream(final String columnLabel) throws SQLException {
        if(protocol.supportsPBMS()) {
            try {
                return getValueObject(columnLabel).getPBMSStream(protocol);
            } catch (QueryException e) {
                throw SQLExceptionMapper.get(e);
            } catch (IOException e) {
                throw SQLExceptionMapper.getSQLException("Could not read back the data using http", e);
            }
        } else {
            return getValueObject(columnLabel).getBinaryInputStream();
        }
    }

    /**
     * Retrieves the first warning reported by calls on this <code>ResultSet</code> object. Subsequent warnings on this
     * <code>ResultSet</code> object will be chained to the <code>SQLWarning</code> object that this method returns.
     * <p/>
     * <P>The warning chain is automatically cleared each time a new row is read.  This method may not be called on a
     * <code>ResultSet</code> object that has been closed; doing so will cause an <code>SQLException</code> to be
     * thrown.
     * <p/>
     * <B>Note:</B> This warning chain only covers warnings caused by <code>ResultSet</code> methods.  Any warning
     * caused by <code>Statement</code> methods (such as reading OUT parameters) will be chained on the
     * <code>Statement</code> object.
     *
     * @return the first <code>SQLWarning</code> object reported or <code>null</code> if there are none
     * @throws java.sql.SQLException if a database access error occurs or this method is called on a closed result set
     */
    public SQLWarning getWarnings() throws SQLException {
        if (this.statement == null || warningsCleared)
            return null;
        return this.statement.getWarnings();
    }

    /**
     * Clears all warnings reported on this <code>ResultSet</code> object. After this method is called, the method
     * <code>getWarnings</code> returns <code>null</code> until a new warning is reported for this
     * <code>ResultSet</code> object.
     *
     * @throws java.sql.SQLException if a database access error occurs or this method is called on a closed result set
     */
    public void clearWarnings() throws SQLException {
        warningsCleared = true;
    }

    /**
     * Retrieves the name of the SQL cursor used by this <code>ResultSet</code> object.
     * <p/>
     * <P>In SQL, a result table is retrieved through a cursor that is named. The current row of a result set can be
     * updated or deleted using a positioned update/delete statement that references the cursor name. To insure that the
     * cursor has the proper isolation level to support update, the cursor's <code>SELECT</code> statement should be of
     * the form <code>SELECT FOR UPDATE</code>. If <code>FOR UPDATE</code> is omitted, the positioned updates may fail.
     * <p/>
     * <P>The JDBC API supports this SQL feature by providing the name of the SQL cursor used by a
     * <code>ResultSet</code> object. The current row of a <code>ResultSet</code> object is also the current row of this
     * SQL cursor.
     *
     * @return the SQL name for this <code>ResultSet</code> object's cursor
     * @throws java.sql.SQLException if a database access error occurs or this method is called on a closed result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     */
    public String getCursorName() throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Cursors not supported");
    }

    /**
     * Retrieves the  number, types and properties of this <code>ResultSet</code> object's columns.
     *
     * @return the description of this <code>ResultSet</code> object's columns
     * @throws java.sql.SQLException if a database access error occurs or this method is called on a closed result set
     */
    public ResultSetMetaData getMetaData() throws SQLException {
        return new MySQLResultSetMetaData(queryResult.getColumnInformation());
    }

    /**
     * <p>Gets the value of the designated column in the current row of this <code>ResultSet</code> object as an
     * <code>Object</code> in the Java programming language.
     * <p/>
     * <p>This method will return the value of the given column as a Java object.  The type of the Java object will be
     * the default Java object type corresponding to the column's SQL type, following the mapping for built-in types
     * specified in the JDBC specification. If the value is an SQL <code>NULL</code>, the driver returns a Java
     * <code>null</code>.
     * <p/>
     * <p>This method may also be used to read database-specific abstract data types.
     * <p/>
     * In the JDBC 2.0 API, the behavior of method <code>getObject</code> is extended to materialize data of SQL
     * user-defined types.
     * <p/>
     * If <code>Connection.getTypeMap</code> does not throw a <code>SQLFeatureNotSupportedException</code>, then when a
     * column contains a structured or distinct value, the behavior of this method is as if it were a call to:
     * <code>getObject(columnIndex, this.getStatement().getConnection().getTypeMap())</code>.
     * <p/>
     * If <code>Connection.getTypeMap</code> does throw a <code>SQLFeatureNotSupportedException</code>, then structured
     * values are not supported, and distinct values are mapped to the default Java class as determined by the
     * underlying SQL type of the DISTINCT type.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return a <code>java.lang.Object</code> holding the column value
     * @throws java.sql.SQLException if the columnIndex is not valid; if a database access error occurs or this method
     *                               is called on a closed result set
     */
    public Object getObject(final int columnIndex) throws SQLException {
        try {
            return getValueObject(columnIndex).getObject();
        } catch (ParseException e) {
            throw SQLExceptionMapper.getSQLException("Could not get object: " + e.getMessage(), e);
        }
    }

    /**
     * <p>Gets the value of the designated column in the current row of this <code>ResultSet</code> object as an
     * <code>Object</code> in the Java programming language.
     * <p/>
     * <p>This method will return the value of the given column as a Java object.  The type of the Java object will be
     * the default Java object type corresponding to the column's SQL type, following the mapping for built-in types
     * specified in the JDBC specification. If the value is an SQL <code>NULL</code>, the driver returns a Java
     * <code>null</code>.
     * <p/>
     * This method may also be used to read database-specific abstract data types.
     * <p/>
     * In the JDBC 2.0 API, the behavior of the method <code>getObject</code> is extended to materialize data of SQL
     * user-defined types.  When a column contains a structured or distinct value, the behavior of this method is as if
     * it were a call to: <code>getObject(columnIndex, this.getStatement().getConnection().getTypeMap())</code>.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @return a <code>java.lang.Object</code> holding the column value
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs or this method
     *                               is called on a closed result set
     */
    public Object getObject(final String columnLabel) throws SQLException {
        try {
            return getValueObject(columnLabel).getObject();
        } catch (ParseException e) {
            throw SQLExceptionMapper.getSQLException("Could not get object: " + e.getMessage(), e);
        }
    }

    /**
     * Maps the given <code>ResultSet</code> column label to its <code>ResultSet</code> column index.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @return the column index of the given column name
     * @throws java.sql.SQLException if the <code>ResultSet</code> object does not contain a column labeled
     *                               <code>columnLabel</code>, a database access error occurs or this method is called
     *                               on a closed result set
     */
    public int findColumn(final String columnLabel) throws SQLException {
        if (this.queryResult.getResultSetType() == ResultSetType.SELECT) {
             return columnNameMap.getIndex(columnLabel) +1;
        }
        throw SQLExceptionMapper.getSQLException("Cannot get column id of update result sets");
    }

    /**
     * Retrieves the value of the designated column in the current row of this <code>ResultSet</code> object as a
     * <code>java.io.Reader</code> object.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return a <code>java.io.Reader</code> object that contains the column value; if the value is SQL
     *         <code>NULL</code>, the value returned is <code>null</code> in the Java programming language.
     * @throws java.sql.SQLException if the columnIndex is not valid; if a database access error occurs or this method
     *                               is called on a closed result set
     * @since 1.2
     */
    public Reader getCharacterStream(final int columnIndex) throws SQLException {
        return new StringReader(getValueObject(columnIndex).getString());
    }

    /**
     * Retrieves the value of the designated column in the current row of this <code>ResultSet</code> object as a
     * <code>java.io.Reader</code> object.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @return a <code>java.io.Reader</code> object that contains the column value; if the value is SQL
     *         <code>NULL</code>, the value returned is <code>null</code> in the Java programming language
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs or this method
     *                               is called on a closed result set
     * @since 1.2
     */
    public Reader getCharacterStream(final String columnLabel) throws SQLException {
        return new StringReader(getValueObject(columnLabel).getString());
    }

    /**
     * Retrieves the value of the designated column in the current row of this <code>ResultSet</code> object as a
     * <code>java.math.BigDecimal</code> with full precision.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return the column value (full precision); if the value is SQL <code>NULL</code>, the value returned is
     *         <code>null</code> in the Java programming language.
     * @throws java.sql.SQLException if the columnIndex is not valid; if a database access error occurs or this method
     *                               is called on a closed result set
     * @since 1.2
     */
    public BigDecimal getBigDecimal(final int columnIndex) throws SQLException {
        return getValueObject(columnIndex).getBigDecimal();
    }

    /**
     * Retrieves the value of the designated column in the current row of this <code>ResultSet</code> object as a
     * <code>java.math.BigDecimal</code> with full precision.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @return the column value (full precision); if the value is SQL <code>NULL</code>, the value returned is
     *         <code>null</code> in the Java programming language.
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs or this method
     *                               is called on a closed result set
     * @since 1.2
     */
    public BigDecimal getBigDecimal(final String columnLabel) throws SQLException {
        return getValueObject(columnLabel).getBigDecimal();
    }

    /**
     * Retrieves whether the cursor is before the first row in this <code>ResultSet</code> object.
     * <p/>
     * <strong>Note:</strong>Support for the <code>isBeforeFirst</code> method is optional for <code>ResultSet</code>s
     * with a result set type of <code>TYPE_FORWARD_ONLY</code>
     *
     * @return <code>true</code> if the cursor is before the first row; <code>false</code> if the cursor is at any other
     *         position or the result set contains no rows
     * @throws java.sql.SQLException if a database access error occurs or this method is called on a closed result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public boolean isBeforeFirst() throws SQLException {
        return (queryResult.getResultSetType() == ResultSetType.SELECT  && ((SelectQueryResult) queryResult).isBeforeFirst());
    }

    /**
     * Retrieves whether the cursor is after the last row in this <code>ResultSet</code> object.
     * <p/>
     * <strong>Note:</strong>Support for the <code>isAfterLast</code> method is optional for <code>ResultSet</code>s
     * with a result set type of <code>TYPE_FORWARD_ONLY</code>
     *
     * @return <code>true</code> if the cursor is after the last row; <code>false</code> if the cursor is at any other
     *         position or the result set contains no rows
     * @throws java.sql.SQLException if a database access error occurs or this method is called on a closed result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public boolean isAfterLast() throws SQLException {
        return queryResult.getResultSetType() == ResultSetType.SELECT
                && ((SelectQueryResult) queryResult).isAfterLast();
    }

    /**
     * Retrieves whether the cursor is on the first row of this <code>ResultSet</code> object.
     * <p/>
     * <strong>Note:</strong>Support for the <code>isFirst</code> method is optional for <code>ResultSet</code>s with a
     * result set type of <code>TYPE_FORWARD_ONLY</code>
     *
     * @return <code>true</code> if the cursor is on the first row; <code>false</code> otherwise
     * @throws java.sql.SQLException if a database access error occurs or this method is called on a closed result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public boolean isFirst() throws SQLException {
        return queryResult.getResultSetType() != ResultSetType.MODIFY
                && ((SelectQueryResult) queryResult).getRowPointer() == 0;
    }

    /**
     * Retrieves whether the cursor is on the last row of this <code>ResultSet</code> object. <strong>Note:</strong>
     * Calling the method <code>isLast</code> may be expensive because the JDBC driver might need to fetch ahead one row
     * in order to determine whether the current row is the last row in the result set.
     * <p/>
     * <strong>Note:</strong> Support for the <code>isLast</code> method is optional for <code>ResultSet</code>s with a
     * result set type of <code>TYPE_FORWARD_ONLY</code>
     *
     * @return <code>true</code> if the cursor is on the last row; <code>false</code> otherwise
     * @throws java.sql.SQLException if a database access error occurs or this method is called on a closed result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public boolean isLast() throws SQLException {
        if (queryResult.getResultSetType() == ResultSetType.SELECT)
        {
            if (queryResult instanceof CachedSelectResult) {
               return ((SelectQueryResult) queryResult).getRowPointer() == queryResult.getRows() - 1;
            }
        }
        throw new SQLFeatureNotSupportedException("isLast is not supported for TYPE_FORWARD_ONLY result sets");
    }

    /**
     * Moves the cursor to the front of this <code>ResultSet</code> object, just before the first row. This method has
     * no effect if the result set contains no rows.
     *
     * @throws java.sql.SQLException if a database access error occurs; this method is called on a closed result set or
     *                               the result set type is <code>TYPE_FORWARD_ONLY</code>
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public void beforeFirst() throws SQLException {
        if (queryResult.getResultSetType() == ResultSetType.SELECT) {
            if (!(queryResult instanceof CachedSelectResult)) {
              throw new SQLException("Invalid operation for result set type TYPE_FORWARD_ONLY");
            }
            ((SelectQueryResult) queryResult).moveRowPointerTo(-1);
        }
    }

    /**
     * Moves the cursor to the end of this <code>ResultSet</code> object, just after the last row. This method has no
     * effect if the result set contains no rows.
     *
     * @throws java.sql.SQLException if a database access error occurs; this method is called on a closed result set or
     *                               the result set type is <code>TYPE_FORWARD_ONLY</code>
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public void afterLast() throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Cannot move after last row");
    }

    /**
     * Moves the cursor to the first row in this <code>ResultSet</code> object.
     *
     * @return <code>true</code> if the cursor is on a valid row; <code>false</code> if there are no rows in the result
     *         set
     * @throws java.sql.SQLException if a database access error occurs; this method is called on a closed result set or
     *                               the result set type is <code>TYPE_FORWARD_ONLY</code>
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public boolean first() throws SQLException {
        if (queryResult.getResultSetType() == ResultSetType.SELECT && queryResult.getRows() > 0) {
             if (!(queryResult instanceof CachedSelectResult)) {
              throw new SQLException("Invalid operation for result set type TYPE_FORWARD_ONLY");
            }
            ((SelectQueryResult) queryResult).moveRowPointerTo(0);
            return true;
        }
        return false;
    }

    /**
     * Moves the cursor to the last row in this <code>ResultSet</code> object.
     *
     * @return <code>true</code> if the cursor is on a valid row; <code>false</code> if there are no rows in the result
     *         set
     * @throws java.sql.SQLException if a database access error occurs; this method is called on a closed result set or
     *                               the result set type is <code>TYPE_FORWARD_ONLY</code>
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public boolean last() throws SQLException {
        if (queryResult.getResultSetType() == ResultSetType.SELECT && queryResult.getRows() > 0) {
            ((SelectQueryResult) queryResult).moveRowPointerTo(queryResult.getRows() - 1);
            return true;
        }
        return false;
    }

    /**
     * Retrieves the current row number.  The first row is number 1, the second number 2, and so on.
     * <p/>
     * <strong>Note:</strong>Support for the <code>getRow</code> method is optional for <code>ResultSet</code>s with a
     * result set type of <code>TYPE_FORWARD_ONLY</code>
     *
     * @return the current row number; <code>0</code> if there is no current row
     * @throws java.sql.SQLException if a database access error occurs or this method is called on a closed result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public int getRow() throws SQLException {
        if (queryResult.getResultSetType() == ResultSetType.SELECT) {
            return ((SelectQueryResult) queryResult).getRowPointer() + 1;//+1 since first row is 1, not 0
        }
        return 0;
    }

    /**
     * Moves the cursor to the given row number in this <code>ResultSet</code> object.
     * <p/>
     * <p>If the row number is positive, the cursor moves to the given row number with respect to the beginning of the
     * result set.  The first row is row 1, the second is row 2, and so on.
     * <p/>
     * <p>If the given row number is negative, the cursor moves to an absolute row position with respect to the end of
     * the result set.  For example, calling the method <code>absolute(-1)</code> positions the cursor on the last row;
     * calling the method <code>absolute(-2)</code> moves the cursor to the next-to-last row, and so on.
     * <p/>
     * <p>An attempt to position the cursor beyond the first/last row in the result set leaves the cursor before the
     * first row or after the last row.
     * <p/>
     * <p><B>Note:</B> Calling <code>absolute(1)</code> is the same as calling <code>first()</code>. Calling
     * <code>absolute(-1)</code> is the same as calling <code>last()</code>.
     *
     * @param row the number of the row to which the cursor should move. A positive number indicates the row number
     *            counting from the beginning of the result set; a negative number indicates the row number counting
     *            from the end of the result set
     * @return <code>true</code> if the cursor is moved to a position in this <code>ResultSet</code> object;
     *         <code>false</code> if the cursor is before the first row or after the last row
     * @throws java.sql.SQLException if a database access error occurs; this method is called on a closed result set or
     *                               the result set type is <code>TYPE_FORWARD_ONLY</code>
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public boolean absolute(final int row) throws SQLException {
        if (queryResult.getResultSetType() != ResultSetType.SELECT) {
            return false;
        }
        final SelectQueryResult sqr = (SelectQueryResult) queryResult;
        if (sqr.getRows() > 0) {
            if (row >= 0 && row <= sqr.getRows()) {
                sqr.moveRowPointerTo(row - 1);
                return true;
            }
            if (row < 0) {
                sqr.moveRowPointerTo(sqr.getRows() + row);
            }
            return true;
        }
        return false;
    }

    /**
     * Moves the cursor a relative number of rows, either positive or negative. Attempting to move beyond the first/last
     * row in the result set positions the cursor before/after the the first/last row. Calling <code>relative(0)</code>
     * is valid, but does not change the cursor position.
     * <p/>
     * <p>Note: Calling the method <code>relative(1)</code> is identical to calling the method <code>next()</code> and
     * calling the method <code>relative(-1)</code> is identical to calling the method <code>previous()</code>.
     *
     * @param rows an <code>int</code> specifying the number of rows to move from the current row; a positive number
     *             moves the cursor forward; a negative number moves the cursor backward
     * @return <code>true</code> if the cursor is on a row; <code>false</code> otherwise
     * @throws java.sql.SQLException if a database access error occurs;  this method is called on a closed result set or
     *                               the result set type is <code>TYPE_FORWARD_ONLY</code>
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public boolean relative(final int rows) throws SQLException {
        if (queryResult.getResultSetType() != ResultSetType.SELECT) {
            return false;
        }
        final SelectQueryResult sqr = (SelectQueryResult) queryResult;
        if (queryResult.getRows() > 0) {
            final int newPos = sqr.getRowPointer() + rows;
            if (newPos > -1 && newPos <= queryResult.getRows()) {
                sqr.moveRowPointerTo(newPos);
                return true;
            }
        }
        return false;
    }

    /**
     * Moves the cursor to the previous row in this <code>ResultSet</code> object.
     * <p/>
     * When a call to the <code>previous</code> method returns <code>false</code>, the cursor is positioned before the
     * first row.  Any invocation of a <code>ResultSet</code> method which requires a current row will result in a
     * <code>SQLException</code> being thrown.
     * <p/>
     * If an input stream is open for the current row, a call to the method <code>previous</code> will implicitly close
     * it.  A <code>ResultSet</code> object's warning change is cleared when a new row is read.
     * <p/>
     *
     * @return <code>true</code> if the cursor is now positioned on a valid row; <code>false</code> if the cursor is
     *         positioned before the first row
     * @throws java.sql.SQLException if a database access error occurs; this method is called on a closed result set or
     *                               the result set type is <code>TYPE_FORWARD_ONLY</code>
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public boolean previous() throws SQLException {
        if (queryResult.getResultSetType() != ResultSetType.SELECT) {
            return false;
        }
        final SelectQueryResult sqr = (SelectQueryResult) queryResult;

        if (sqr.isBeforeFirst())
            return false;

        if (sqr.getRows() >= 0) {
            sqr.moveRowPointerTo(sqr.getRowPointer() - 1);
            return !sqr.isBeforeFirst();
        }
        return false;
    }

    /**
     * Gives a hint as to the direction in which the rows in this <code>ResultSet</code> object will be processed. The
     * initial value is determined by the <code>Statement</code> object that produced this <code>ResultSet</code>
     * object. The fetch direction may be changed at any time.
     *
     * @param direction an <code>int</code> specifying the suggested fetch direction; one of
     *                  <code>ResultSet.FETCH_FORWARD</code>, <code>ResultSet.FETCH_REVERSE</code>, or
     *                  <code>ResultSet.FETCH_UNKNOWN</code>
     * @throws java.sql.SQLException if a database access error occurs; this method is called on a closed result set or
     *                               the result set type is <code>TYPE_FORWARD_ONLY</code> and the fetch direction is
     *                               not <code>FETCH_FORWARD</code>
     * @see java.sql.Statement#setFetchDirection
     * @see #getFetchDirection
     * @since 1.2
     */
    public void setFetchDirection(final int direction) throws SQLException {
        // todo: ignored for now
    }

    /**
     * Retrieves the fetch direction for this <code>ResultSet</code> object.
     *
     * @return the current fetch direction for this <code>ResultSet</code> object
     * @throws java.sql.SQLException if a database access error occurs or this method is called on a closed result set
     * @see #setFetchDirection
     * @since 1.2
     */
    public int getFetchDirection() throws SQLException {
        return ResultSet.FETCH_UNKNOWN;
    }

    /**
     * Gives the JDBC driver a hint as to the number of rows that should be fetched from the database when more rows are
     * needed for this <code>ResultSet</code> object. If the fetch size specified is zero, the JDBC driver ignores the
     * value and is free to make its own best guess as to what the fetch size should be.  The default value is set by
     * the <code>Statement</code> object that created the result set.  The fetch size may be changed at any time.
     *
     * @param rows the number of rows to fetch
     * @throws java.sql.SQLException if a database access error occurs; this method is called on a closed result set or
     *                               the condition <code>rows >= 0 </code> is not satisfied
     * @see #getFetchSize
     * @since 1.2
     */
    public void setFetchSize(final int rows) throws SQLException {
        // ignored - we fetch 'em all!
    }

    /**
     * Retrieves the fetch size for this <code>ResultSet</code> object.
     *
     * @return the current fetch size for this <code>ResultSet</code> object
     * @throws java.sql.SQLException if a database access error occurs or this method is called on a closed result set
     * @see #setFetchSize
     * @since 1.2
     */
    public int getFetchSize() throws SQLException {
        return 0;
    }

    /**
     * Retrieves the type of this <code>ResultSet</code> object. The type is determined by the <code>Statement</code>
     * object that created the result set.
     *
     * @return <code>ResultSet.TYPE_FORWARD_ONLY</code>, <code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or
     *         <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     * @throws java.sql.SQLException if a database access error occurs or this method is called on a closed result set
     * @since 1.2
     */
    public int getType() throws SQLException {
        return (queryResult instanceof StreamingSelectResult)? ResultSet.TYPE_FORWARD_ONLY : ResultSet.TYPE_SCROLL_INSENSITIVE;
    }

    /**
     * Retrieves the concurrency mode of this <code>ResultSet</code> object. The concurrency used is determined by the
     * <code>Statement</code> object that created the result set.
     *
     * @return the concurrency type, either <code>ResultSet.CONCUR_READ_ONLY</code> or
     *         <code>ResultSet.CONCUR_UPDATABLE</code>
     * @throws java.sql.SQLException if a database access error occurs or this method is called on a closed result set
     * @since 1.2
     */
    public int getConcurrency() throws SQLException {
        return ResultSet.CONCUR_READ_ONLY;
    }

    /**
     * Retrieves whether the current row has been updated.  The value returned depends on whether or not the result set
     * can detect updates.
     * <p/>
     * <strong>Note:</strong> Support for the <code>rowUpdated</code> method is optional with a result set concurrency
     * of <code>CONCUR_READ_ONLY</code>
     *
     * @return <code>true</code> if the current row is detected to have been visibly updated by the owner or another;
     *         <code>false</code> otherwise
     * @throws java.sql.SQLException if a database access error occurs or this method is called on a closed result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @see java.sql.DatabaseMetaData#updatesAreDetected
     * @since 1.2
     */
    public boolean rowUpdated() throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Detecting row updates are not supported");
    }

    /**
     * Retrieves whether the current row has had an insertion. The value returned depends on whether or not this
     * <code>ResultSet</code> object can detect visible inserts.
     * <p/>
     * <strong>Note:</strong> Support for the <code>rowInserted</code> method is optional with a result set concurrency
     * of <code>CONCUR_READ_ONLY</code>
     *
     * @return <code>true</code> if the current row is detected to have been inserted; <code>false</code> otherwise
     * @throws java.sql.SQLException if a database access error occurs or this method is called on a closed result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @see java.sql.DatabaseMetaData#insertsAreDetected
     * @since 1.2
     */
    public boolean rowInserted() throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Detecting inserts are not supported");
    }

    /**
     * Retrieves whether a row has been deleted.  A deleted row may leave a visible "hole" in a result set.  This method
     * can be used to detect holes in a result set.  The value returned depends on whether or not this
     * <code>ResultSet</code> object can detect deletions.
     * <p/>
     * <strong>Note:</strong> Support for the <code>rowDeleted</code> method is optional with a result set concurrency
     * of <code>CONCUR_READ_ONLY</code>
     *
     * @return <code>true</code> if the current row is detected to have been deleted by the owner or another;
     *         <code>false</code> otherwise
     * @throws java.sql.SQLException if a database access error occurs or this method is called on a closed result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @see java.sql.DatabaseMetaData#deletesAreDetected
     * @since 1.2
     */
    public boolean rowDeleted() throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Row deletes are not supported");
    }

    /**
     * Updates the designated column with a <code>null</code> value.
     * <p/>
     * The updater methods are used to update column values in the current row or the insert row.  The updater methods
     * do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @throws java.sql.SQLException if the columnIndex is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public void updateNull(final int columnIndex) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * Updates the designated column with a <code>boolean</code> value. The updater methods are used to update column
     * values in the current row or the insert row.  The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @throws java.sql.SQLException if the columnIndex is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public void updateBoolean(final int columnIndex, final boolean x) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * Updates the designated column with a <code>byte</code> value. The updater methods are used to update column
     * values in the current row or the insert row.  The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @throws java.sql.SQLException if the columnIndex is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public void updateByte(final int columnIndex, final byte x) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * Updates the designated column with a <code>short</code> value. The updater methods are used to update column
     * values in the current row or the insert row.  The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @throws java.sql.SQLException if the columnIndex is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public void updateShort(final int columnIndex, final short x) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * Updates the designated column with an <code>int</code> value. The updater methods are used to update column
     * values in the current row or the insert row.  The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @throws java.sql.SQLException if the columnIndex is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public void updateInt(final int columnIndex, final int x) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * Updates the designated column with a <code>long</code> value. The updater methods are used to update column
     * values in the current row or the insert row.  The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @throws java.sql.SQLException if the columnIndex is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public void updateLong(final int columnIndex, final long x) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * Updates the designated column with a <code>float</code> value. The updater methods are used to update column
     * values in the current row or the insert row.  The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @throws java.sql.SQLException if the columnIndex is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public void updateFloat(final int columnIndex, final float x) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * Updates the designated column with a <code>double</code> value. The updater methods are used to update column
     * values in the current row or the insert row.  The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @throws java.sql.SQLException if the columnIndex is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public void updateDouble(final int columnIndex, final double x) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * Updates the designated column with a <code>java.math.BigDecimal</code> value. The updater methods are used to
     * update column values in the current row or the insert row.  The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code> methods are called to update the
     * database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @throws java.sql.SQLException if the columnIndex is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public void updateBigDecimal(final int columnIndex, final BigDecimal x) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * Updates the designated column with a <code>String</code> value. The updater methods are used to update column
     * values in the current row or the insert row.  The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @throws java.sql.SQLException if the columnIndex is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public void updateString(final int columnIndex, final String x) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * Updates the designated column with a <code>byte</code> array value. The updater methods are used to update column
     * values in the current row or the insert row.  The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @throws java.sql.SQLException if the columnIndex is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public void updateBytes(final int columnIndex, final byte[] x) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * Updates the designated column with a <code>java.sql.Date</code> value. The updater methods are used to update
     * column values in the current row or the insert row.  The updater methods do not update the underlying database;
     * instead the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @throws java.sql.SQLException if the columnIndex is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public void updateDate(final int columnIndex, final Date x) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * Updates the designated column with a <code>java.sql.Time</code> value. The updater methods are used to update
     * column values in the current row or the insert row.  The updater methods do not update the underlying database;
     * instead the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @throws java.sql.SQLException if the columnIndex is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public void updateTime(final int columnIndex, final Time x) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * Updates the designated column with a <code>java.sql.Timestamp</code> value. The updater methods are used to
     * update column values in the current row or the insert row.  The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code> methods are called to update the
     * database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @throws java.sql.SQLException if the columnIndex is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public void updateTimestamp(final int columnIndex, final Timestamp x) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * Updates the designated column with an ascii stream value, which will have the specified number of bytes. The
     * updater methods are used to update column values in the current row or the insert row.  The updater methods do
     * not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @param length      the length of the stream
     * @throws java.sql.SQLException if the columnIndex is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public void updateAsciiStream(final int columnIndex, final InputStream x, final int length) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * Updates the designated column with a binary stream value, which will have the specified number of bytes. The
     * updater methods are used to update column values in the current row or the insert row.  The updater methods do
     * not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @param length      the length of the stream
     * @throws java.sql.SQLException if the columnIndex is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public void updateBinaryStream(final int columnIndex, final InputStream x, final int length) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * Updates the designated column with a character stream value, which will have the specified number of bytes. The
     * updater methods are used to update column values in the current row or the insert row.  The updater methods do
     * not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @param length      the length of the stream
     * @throws java.sql.SQLException if the columnIndex is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public void updateCharacterStream(final int columnIndex, final Reader x, final int length) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * Updates the designated column with an <code>Object</code> value. The updater methods are used to update column
     * values in the current row or the insert row.  The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     * <p/>
     * If the second argument is an <code>InputStream</code> then the stream must contain the number of bytes specified
     * by scaleOrLength.  If the second argument is a <code>Reader</code> then the reader must contain the number of
     * characters specified by scaleOrLength. If these conditions are not true the driver will generate a
     * <code>SQLException</code> when the statement is executed.
     *
     * @param columnIndex   the first column is 1, the second is 2, ...
     * @param x             the new column value
     * @param scaleOrLength for an object of <code>java.math.BigDecimal</code> , this is the number of digits after the
     *                      decimal point. For Java Object types <code>InputStream</code> and <code>Reader</code>, this
     *                      is the length of the data in the stream or reader.  For all other types, this value will be
     *                      ignored.
     * @throws java.sql.SQLException if the columnIndex is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public void updateObject(final int columnIndex, final Object x, final int scaleOrLength) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * Updates the designated column with an <code>Object</code> value. The updater methods are used to update column
     * values in the current row or the insert row.  The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @throws java.sql.SQLException if the columnIndex is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public void updateObject(final int columnIndex, final Object x) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * Updates the designated column with a <code>null</code> value. The updater methods are used to update column
     * values in the current row or the insert row.  The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public void updateNull(final String columnLabel) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * Updates the designated column with a <code>boolean</code> value. The updater methods are used to update column
     * values in the current row or the insert row.  The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @param x           the new column value
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public void updateBoolean(final String columnLabel, final boolean x) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * Updates the designated column with a <code>byte</code> value. The updater methods are used to update column
     * values in the current row or the insert row.  The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @param x           the new column value
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public void updateByte(final String columnLabel, final byte x) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * Updates the designated column with a <code>short</code> value. The updater methods are used to update column
     * values in the current row or the insert row.  The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @param x           the new column value
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public void updateShort(final String columnLabel, final short x) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * Updates the designated column with an <code>int</code> value. The updater methods are used to update column
     * values in the current row or the insert row.  The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @param x           the new column value
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public void updateInt(final String columnLabel, final int x) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * Updates the designated column with a <code>long</code> value. The updater methods are used to update column
     * values in the current row or the insert row.  The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @param x           the new column value
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public void updateLong(final String columnLabel, final long x) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * Updates the designated column with a <code>float </code> value. The updater methods are used to update column
     * values in the current row or the insert row.  The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @param x           the new column value
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public void updateFloat(final String columnLabel, final float x) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * Updates the designated column with a <code>double</code> value. The updater methods are used to update column
     * values in the current row or the insert row.  The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @param x           the new column value
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public void updateDouble(final String columnLabel, final double x) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * Updates the designated column with a <code>java.sql.BigDecimal</code> value. The updater methods are used to
     * update column values in the current row or the insert row.  The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code> methods are called to update the
     * database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @param x           the new column value
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public void updateBigDecimal(final String columnLabel, final BigDecimal x) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * Updates the designated column with a <code>String</code> value. The updater methods are used to update column
     * values in the current row or the insert row.  The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @param x           the new column value
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public void updateString(final String columnLabel, final String x) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * Updates the designated column with a byte array value.
     * <p/>
     * The updater methods are used to update column values in the current row or the insert row.  The updater methods
     * do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @param x           the new column value
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public void updateBytes(final String columnLabel, final byte[] x) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * Updates the designated column with a <code>java.sql.Date</code> value. The updater methods are used to update
     * column values in the current row or the insert row.  The updater methods do not update the underlying database;
     * instead the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @param x           the new column value
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public void updateDate(final String columnLabel, final Date x) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * Updates the designated column with a <code>java.sql.Time</code> value. The updater methods are used to update
     * column values in the current row or the insert row.  The updater methods do not update the underlying database;
     * instead the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @param x           the new column value
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public void updateTime(final String columnLabel, final Time x) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * Updates the designated column with a <code>java.sql.Timestamp</code> value. The updater methods are used to
     * update column values in the current row or the insert row.  The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code> methods are called to update the
     * database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @param x           the new column value
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public void updateTimestamp(final String columnLabel, final Timestamp x) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * Updates the designated column with an ascii stream value, which will have the specified number of bytes. The
     * updater methods are used to update column values in the current row or the insert row.  The updater methods do
     * not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @param x           the new column value
     * @param length      the length of the stream
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public void updateAsciiStream(final String columnLabel, final InputStream x, final int length) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * Updates the designated column with a binary stream value, which will have the specified number of bytes. The
     * updater methods are used to update column values in the current row or the insert row.  The updater methods do
     * not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @param x           the new column value
     * @param length      the length of the stream
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public void updateBinaryStream(final String columnLabel, final InputStream x, final int length) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * Updates the designated column with a character stream value, which will have the specified number of bytes. The
     * updater methods are used to update column values in the current row or the insert row.  The updater methods do
     * not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @param reader      the <code>java.io.Reader</code> object containing the new column value
     * @param length      the length of the stream
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public void updateCharacterStream(final String columnLabel, final Reader reader, final int length) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * Updates the designated column with an <code>Object</code> value. The updater methods are used to update column
     * values in the current row or the insert row.  The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     * <p/>
     * If the second argument is an <code>InputStream</code> then the stream must contain the number of bytes specified
     * by scaleOrLength.  If the second argument is a <code>Reader</code> then the reader must contain the number of
     * characters specified by scaleOrLength. If these conditions are not true the driver will generate a
     * <code>SQLException</code> when the statement is executed.
     *
     * @param columnLabel   the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                      specified, then the label is the name of the column
     * @param x             the new column value
     * @param scaleOrLength for an object of <code>java.math.BigDecimal</code> , this is the number of digits after the
     *                      decimal point. For Java Object types <code>InputStream</code> and <code>Reader</code>, this
     *                      is the length of the data in the stream or reader.  For all other types, this value will be
     *                      ignored.
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public void updateObject(final String columnLabel, final Object x, final int scaleOrLength) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * Updates the designated column with an <code>Object</code> value. The updater methods are used to update column
     * values in the current row or the insert row.  The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @param x           the new column value
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public void updateObject(final String columnLabel, final Object x) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * Inserts the contents of the insert row into this <code>ResultSet</code> object and into the database. The cursor
     * must be on the insert row when this method is called.
     *
     * @throws java.sql.SQLException if a database access error occurs; the result set concurrency is
     *                               <code>CONCUR_READ_ONLY</code>, this method is called on a closed result set, if
     *                               this method is called when the cursor is not on the insert row, or if not all of
     *                               non-nullable columns in the insert row have been given a non-null value
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public void insertRow() throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * Updates the underlying database with the new contents of the current row of this <code>ResultSet</code> object.
     * This method cannot be called when the cursor is on the insert row.
     *
     * @throws java.sql.SQLException if a database access error occurs; the result set concurrency is
     *                               <code>CONCUR_READ_ONLY</code>; this method is called on a closed result set or if
     *                               this method is called when the cursor is on the insert row
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public void updateRow() throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * Deletes the current row from this <code>ResultSet</code> object and from the underlying database.  This method
     * cannot be called when the cursor is on the insert row.
     *
     * @throws java.sql.SQLException if a database access error occurs; the result set concurrency is
     *                               <code>CONCUR_READ_ONLY</code>; this method is called on a closed result set or if
     *                               this method is called when the cursor is on the insert row
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public void deleteRow() throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * Refreshes the current row with its most recent value in the database.  This method cannot be called when the
     * cursor is on the insert row.
     * <p/>
     * <P>The <code>refreshRow</code> method provides a way for an application to explicitly tell the JDBC driver to
     * refetch a row(s) from the database.  An application may want to call <code>refreshRow</code> when caching or
     * prefetching is being done by the JDBC driver to fetch the latest value of a row from the database.  The JDBC
     * driver may actually refresh multiple rows at once if the fetch size is greater than one.
     * <p/>
     * <P> All values are refetched subject to the transaction isolation level and cursor sensitivity.  If
     * <code>refreshRow</code> is called after calling an updater method, but before calling the method
     * <code>updateRow</code>, then the updates made to the row are lost.  Calling the method <code>refreshRow</code>
     * frequently will likely slow performance.
     *
     * @throws java.sql.SQLException if a database access error occurs; this method is called on a closed result set;
     *                               the result set type is <code>TYPE_FORWARD_ONLY</code> or if this method is called
     *                               when the cursor is on the insert row
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method or this method is not supported for
     *                               the specified result set type and result set concurrency.
     * @since 1.2
     */
    public void refreshRow() throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Row refresh is not supported");
    }

    /**
     * Cancels the updates made to the current row in this <code>ResultSet</code> object. This method may be called
     * after calling an updater method(s) and before calling the method <code>updateRow</code> to roll back the updates
     * made to a row.  If no updates have been made or <code>updateRow</code> has already been called, this method has
     * no effect.
     *
     * @throws java.sql.SQLException if a database access error occurs; this method is called on a closed result set;
     *                               the result set concurrency is <code>CONCUR_READ_ONLY</code> or if this method is
     *                               called when the cursor is on the insert row
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public void cancelRowUpdates() throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * Moves the cursor to the insert row.  The current cursor position is remembered while the cursor is positioned on
     * the insert row.
     * <p/>
     * The insert row is a special row associated with an updatable result set.  It is essentially a buffer where a new
     * row may be constructed by calling the updater methods prior to inserting the row into the result set.
     * <p/>
     * Only the updater, getter, and <code>insertRow</code> methods may be called when the cursor is on the insert row.
     * All of the columns in a result set must be given a value each time this method is called before calling
     * <code>insertRow</code>. An updater method must be called before a getter method can be called on a column value.
     *
     * @throws java.sql.SQLException if a database access error occurs; this method is called on a closed result set or
     *                               the result set concurrency is <code>CONCUR_READ_ONLY</code>
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public void moveToInsertRow() throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * Moves the cursor to the remembered cursor position, usually the current row.  This method has no effect if the
     * cursor is not on the insert row.
     *
     * @throws java.sql.SQLException if a database access error occurs; this method is called on a closed result set or
     *                               the result set concurrency is <code>CONCUR_READ_ONLY</code>
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public void moveToCurrentRow() throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * Retrieves the <code>Statement</code> object that produced this <code>ResultSet</code> object. If the result set
     * was generated some other way, such as by a <code>DatabaseMetaData</code> method, this method  may return
     * <code>null</code>.
     *
     * @return the <code>Statment</code> object that produced this <code>ResultSet</code> object or <code>null</code> if
     *         the result set was produced some other way
     * @throws java.sql.SQLException if a database access error occurs or this method is called on a closed result set
     * @since 1.2
     */
    public Statement getStatement() throws SQLException {
        return this.statement;
    }

    /**
     * According to the JDBC4 spec, this is only required for UDT's, and since drizzle does not support UDTs, this
     * method ignores the map parameter
     * <p/>
     * Retrieves the value of the designated column in the current row of this <code>ResultSet</code> object as an
     * <code>Object</code> in the Java programming language. If the value is an SQL <code>NULL</code>, the driver
     * returns a Java <code>null</code>. This method uses the given <code>Map</code> object for the custom mapping of
     * the SQL structured or distinct type that is being retrieved.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param map         a <code>java.util.Map</code> object that contains the mapping from SQL type names to classes
     *                    in the Java programming language
     * @return an <code>Object</code> in the Java programming language representing the SQL value
     * @throws java.sql.SQLException if the columnIndex is not valid; if a database access error occurs or this method
     *                               is called on a closed result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public Object getObject(final int columnIndex, final Map<String, Class<?>> map) throws SQLException {
        return getObject(columnIndex);
    }

    /**
     * Retrieves the value of the designated column in the current row of this <code>ResultSet</code> object as a
     * <code>Ref</code> object in the Java programming language.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return a <code>Ref</code> object representing an SQL <code>REF</code> value
     * @throws java.sql.SQLException if the columnIndex is not valid; if a database access error occurs or this method
     *                               is called on a closed result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public Ref getRef(final int columnIndex) throws SQLException {
        // TODO: figure out what REF's are and implement this method
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * Retrieves the value of the designated column in the current row of this <code>ResultSet</code> object as a
     * <code>Blob</code> object in the Java programming language.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return a <code>Blob</code> object representing the SQL <code>BLOB</code> value in the specified column
     * @throws java.sql.SQLException if the columnIndex is not valid; if a database access error occurs or this method
     *                               is called on a closed result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public Blob getBlob(final int columnIndex) throws SQLException {
        byte[] bytes = getValueObject(columnIndex).getBytes();
        if (bytes == null)
            return null;
        return new MySQLBlob(bytes);
    }

    /**
     * Retrieves the value of the designated column in the current row of this <code>ResultSet</code> object as a
     * <code>Clob</code> object in the Java programming language.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return a <code>Clob</code> object representing the SQL <code>CLOB</code> value in the specified column
     * @throws java.sql.SQLException if the columnIndex is not valid; if a database access error occurs or this method
     *                               is called on a closed result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public Clob getClob(final int columnIndex) throws SQLException {
        byte[] bytes = getValueObject(columnIndex).getBytes();
        if (bytes == null)
            return null;
        return new MySQLClob(bytes);
    }

    /**
     * Retrieves the value of the designated column in the current row of this <code>ResultSet</code> object as an
     * <code>Array</code> object in the Java programming language.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return an <code>Array</code> object representing the SQL <code>ARRAY</code> value in the specified column
     * @throws java.sql.SQLException if the columnIndex is not valid; if a database access error occurs or this method
     *                               is called on a closed result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public Array getArray(final int columnIndex) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Arrays are not supported");
    }

    /**
     * According to the JDBC4 spec, this is only required for UDT's, and since drizzle does not support UDTs, this
     * method ignores the map parameter
     * <p/>
     * Retrieves the value of the designated column in the current row of this <code>ResultSet</code> object as an
     * <code>Object</code> in the Java programming language. If the value is an SQL <code>NULL</code>, the driver
     * returns a Java <code>null</code>. This method uses the specified <code>Map</code> object for custom mapping if
     * appropriate.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @param map         a <code>java.util.Map</code> object that contains the mapping from SQL type names to classes
     *                    in the Java programming language
     * @return an <code>Object</code> representing the SQL value in the specified column
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs or this method
     *                               is called on a closed result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public Object getObject(final String columnLabel, final Map<String, Class<?>> map) throws SQLException {
        //TODO: implement this
        throw SQLExceptionMapper.getFeatureNotSupportedException("Type map getting is not supported");
    }

    /**
     * Retrieves the value of the designated column in the current row of this <code>ResultSet</code> object as a
     * <code>Ref</code> object in the Java programming language.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @return a <code>Ref</code> object representing the SQL <code>REF</code> value in the specified column
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs or this method
     *                               is called on a closed result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public Ref getRef(final String columnLabel) throws SQLException {
        // TODO see getRef(int)
        throw SQLExceptionMapper.getFeatureNotSupportedException("Getting REFs not supported");
    }

    /**
     * Retrieves the value of the designated column in the current row of this <code>ResultSet</code> object as a
     * <code>Blob</code> object in the Java programming language.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @return a <code>Blob</code> object representing the SQL <code>BLOB</code> value in the specified column
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs or this method
     *                               is called on a closed result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public Blob getBlob(final String columnLabel) throws SQLException {
        byte[] bytes = getValueObject(columnLabel).getBytes();
        if (bytes == null)
            return null;
        return new MySQLBlob(bytes);
    }

    /**
     * Retrieves the value of the designated column in the current row of this <code>ResultSet</code> object as a
     * <code>Clob</code> object in the Java programming language.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @return a <code>Clob</code> object representing the SQL <code>CLOB</code> value in the specified column
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs or this method
     *                               is called on a closed result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public Clob getClob(final String columnLabel) throws SQLException {
         byte[] bytes = getValueObject(columnLabel).getBytes();
        if (bytes == null)
            return null;
        return new MySQLClob(bytes);
    }

    /**
     * Retrieves the value of the designated column in the current row of this <code>ResultSet</code> object as an
     * <code>Array</code> object in the Java programming language.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @return an <code>Array</code> object representing the SQL <code>ARRAY</code> value in the specified column
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs or this method
     *                               is called on a closed result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public Array getArray(final String columnLabel) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Arrays are not supported");
    }

    /**
     * Retrieves the value of the designated column in the current row of this <code>ResultSet</code> object as a
     * <code>java.sql.Date</code> object in the Java programming language. This method uses the given calendar to
     * construct an appropriate millisecond value for the date if the underlying database does not store timezone
     * information.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param cal         the <code>java.util.Calendar</code> object to use in constructing the date
     * @return the column value as a <code>java.sql.Date</code> object; if the value is SQL <code>NULL</code>, the value
     *         returned is <code>null</code> in the Java programming language
     * @throws java.sql.SQLException if the columnIndex is not valid; if a database access error occurs or this method
     *                               is called on a closed result set
     * @since 1.2
     */
    public Date getDate(final int columnIndex, final Calendar cal) throws SQLException {
        try {
            return getValueObject(columnIndex).getDate(cal);
        } catch (ParseException e) {
            throw SQLExceptionMapper.getSQLException("Could not parse as date");
        }
    }

    /**
     * Retrieves the value of the designated column in the current row of this <code>ResultSet</code> object as a
     * <code>java.sql.Date</code> object in the Java programming language. This method uses the given calendar to
     * construct an appropriate millisecond value for the date if the underlying database does not store timezone
     * information.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @param cal         the <code>java.util.Calendar</code> object to use in constructing the date
     * @return the column value as a <code>java.sql.Date</code> object; if the value is SQL <code>NULL</code>, the value
     *         returned is <code>null</code> in the Java programming language
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs or this method
     *                               is called on a closed result set
     * @since 1.2
     */
    public Date getDate(final String columnLabel, final Calendar cal) throws SQLException {
        try {
            return getValueObject(columnLabel).getDate(cal);
        } catch (ParseException e) {
            throw SQLExceptionMapper.getSQLException("Could not parse as date");
        }
    }

    /**
     * Retrieves the value of the designated column in the current row of this <code>ResultSet</code> object as a
     * <code>java.sql.Time</code> object in the Java programming language. This method uses the given calendar to
     * construct an appropriate millisecond value for the time if the underlying database does not store timezone
     * information.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param cal         the <code>java.util.Calendar</code> object to use in constructing the time
     * @return the column value as a <code>java.sql.Time</code> object; if the value is SQL <code>NULL</code>, the value
     *         returned is <code>null</code> in the Java programming language
     * @throws java.sql.SQLException if the columnIndex is not valid; if a database access error occurs or this method
     *                               is called on a closed result set
     * @since 1.2
     */
    public Time getTime(final int columnIndex, final Calendar cal) throws SQLException {
        try {
            return getValueObject(columnIndex).getTime(cal);
        } catch (ParseException e) {
            throw SQLExceptionMapper.getSQLException("Could not parse time", e);
        }
    }

    /**
     * Retrieves the value of the designated column in the current row of this <code>ResultSet</code> object as a
     * <code>java.sql.Time</code> object in the Java programming language. This method uses the given calendar to
     * construct an appropriate millisecond value for the time if the underlying database does not store timezone
     * information.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @param cal         the <code>java.util.Calendar</code> object to use in constructing the time
     * @return the column value as a <code>java.sql.Time</code> object; if the value is SQL <code>NULL</code>, the value
     *         returned is <code>null</code> in the Java programming language
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs or this method
     *                               is called on a closed result set
     * @since 1.2
     */
    public Time getTime(final String columnLabel, final Calendar cal) throws SQLException {
        return getTime(findColumn(columnLabel),cal);
    }

    /**
     * Retrieves the value of the designated column in the current row of this <code>ResultSet</code> object as a
     * <code>java.sql.Timestamp</code> object in the Java programming language. This method uses the given calendar to
     * construct an appropriate millisecond value for the timestamp if the underlying database does not store timezone
     * information.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param cal         the <code>java.util.Calendar</code> object to use in constructing the timestamp
     * @return the column value as a <code>java.sql.Timestamp</code> object; if the value is SQL <code>NULL</code>, the
     *         value returned is <code>null</code> in the Java programming language
     * @throws java.sql.SQLException if the columnIndex is not valid; if a database access error occurs or this method
     *                               is called on a closed result set
     * @since 1.2
     */
    public Timestamp getTimestamp(final int columnIndex, final Calendar cal) throws SQLException {
        try {
            return new Timestamp(getValueObject(columnIndex).getTimestamp(cal).getTime());
        } catch (ParseException e) {
            throw SQLExceptionMapper.getSQLException("Could not parse timestamp",e);
        }
    }

    /**
     * Retrieves the value of the designated column in the current row of this <code>ResultSet</code> object as a
     * <code>java.sql.Timestamp</code> object in the Java programming language. This method uses the given calendar to
     * construct an appropriate millisecond value for the timestamp if the underlying database does not store timezone
     * information.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @param cal         the <code>java.util.Calendar</code> object to use in constructing the date
     * @return the column value as a <code>java.sql.Timestamp</code> object; if the value is SQL <code>NULL</code>, the
     *         value returned is <code>null</code> in the Java programming language
     * @throws java.sql.SQLException if the columnLabel is not valid or if a database access error occurs or this method
     *                               is called on a closed result set
     * @since 1.2
     */
    public Timestamp getTimestamp(final String columnLabel, final Calendar cal) throws SQLException {
        try {
            return new Timestamp(getValueObject(columnLabel).getTimestamp(cal).getTime());
        } catch (ParseException e) {
            throw SQLExceptionMapper.getSQLException("Could not parse time", e);
        }
    }

    /**
     * Retrieves the value of the designated column in the current row of this <code>ResultSet</code> object as a
     * <code>java.net.URL</code> object in the Java programming language.
     *
     * @param columnIndex the index of the column 1 is the first, 2 is the second,...
     * @return the column value as a <code>java.net.URL</code> object; if the value is SQL <code>NULL</code>, the value
     *         returned is <code>null</code> in the Java programming language
     * @throws java.sql.SQLException if the columnIndex is not valid; if a database access error occurs; this method is
     *                               called on a closed result set or if a URL is malformed
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.4
     */
    public URL getURL(final int columnIndex) throws SQLException {
        try {
            return new URL(getValueObject(columnIndex).getString());
        } catch (MalformedURLException e) {
            throw SQLExceptionMapper.getSQLException("Could not parse as URL");
        }
    }

    /**
     * Retrieves the value of the designated column in the current row of this <code>ResultSet</code> object as a
     * <code>java.net.URL</code> object in the Java programming language.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @return the column value as a <code>java.net.URL</code> object; if the value is SQL <code>NULL</code>, the value
     *         returned is <code>null</code> in the Java programming language
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs; this method is
     *                               called on a closed result set or if a URL is malformed
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.4
     */
    public URL getURL(final String columnLabel) throws SQLException {
        try {
            return new URL(getValueObject(columnLabel).getString());
        } catch (MalformedURLException e) {
            throw SQLExceptionMapper.getSQLException("Could not parse as URL");
        }
    }

    /**
     * Updates the designated column with a <code>java.sql.Ref</code> value. The updater methods are used to update
     * column values in the current row or the insert row.  The updater methods do not update the underlying database;
     * instead the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @throws java.sql.SQLException if the columnIndex is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.4
     */
    public void updateRef(final int columnIndex, final Ref x) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * Updates the designated column with a <code>java.sql.Ref</code> value. The updater methods are used to update
     * column values in the current row or the insert row.  The updater methods do not update the underlying database;
     * instead the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @param x           the new column value
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.4
     */
    public void updateRef(final String columnLabel, final Ref x) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * Updates the designated column with a <code>java.sql.Blob</code> value. The updater methods are used to update
     * column values in the current row or the insert row.  The updater methods do not update the underlying database;
     * instead the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @throws java.sql.SQLException if the columnIndex is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.4
     */
    public void updateBlob(final int columnIndex, final Blob x) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * Updates the designated column with a <code>java.sql.Blob</code> value. The updater methods are used to update
     * column values in the current row or the insert row.  The updater methods do not update the underlying database;
     * instead the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @param x           the new column value
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.4
     */
    public void updateBlob(final String columnLabel, final Blob x) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * Updates the designated column with a <code>java.sql.Clob</code> value. The updater methods are used to update
     * column values in the current row or the insert row.  The updater methods do not update the underlying database;
     * instead the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @throws java.sql.SQLException if the columnIndex is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.4
     */
    public void updateClob(final int columnIndex, final Clob x) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * Updates the designated column with a <code>java.sql.Clob</code> value. The updater methods are used to update
     * column values in the current row or the insert row.  The updater methods do not update the underlying database;
     * instead the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @param x           the new column value
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.4
     */
    public void updateClob(final String columnLabel, final Clob x) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * Updates the designated column with a <code>java.sql.Array</code> value. The updater methods are used to update
     * column values in the current row or the insert row.  The updater methods do not update the underlying database;
     * instead the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @throws java.sql.SQLException if the columnIndex is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.4
     */
    public void updateArray(final int columnIndex, final Array x) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * Updates the designated column with a <code>java.sql.Array</code> value. The updater methods are used to update
     * column values in the current row or the insert row.  The updater methods do not update the underlying database;
     * instead the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @param x           the new column value
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.4
     */
    public void updateArray(final String columnLabel, final Array x) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * Retrieves the value of the designated column in the current row of this <code>ResultSet</code> object as a
     * <code>java.sql.RowId</code> object in the Java programming language.
     *
     * @param columnIndex the first column is 1, the second 2, ...
     * @return the column value; if the value is a SQL <code>NULL</code> the value returned is <code>null</code>
     * @throws java.sql.SQLException if the columnIndex is not valid; if a database access error occurs or this method
     *                               is called on a closed result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public java.sql.RowId getRowId(final int columnIndex) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("RowIDs not supported");
    }

    /**
     * Retrieves the value of the designated column in the current row of this <code>ResultSet</code> object as a
     * <code>java.sql.RowId</code> object in the Java programming language.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @return the column value ; if the value is a SQL <code>NULL</code> the value returned is <code>null</code>
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs or this method
     *                               is called on a closed result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public java.sql.RowId getRowId(final String columnLabel) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("RowIDs not supported");
    }

    /**
     * Updates the designated column with a <code>RowId</code> value. The updater methods are used to update column
     * values in the current row or the insert row. The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second 2, ...
     * @param x           the column value
     * @throws java.sql.SQLException if the columnIndex is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void updateRowId(final int columnIndex, final java.sql.RowId x) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates are not supported");

    }

    /**
     * Updates the designated column with a <code>RowId</code> value. The updater methods are used to update column
     * values in the current row or the insert row. The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @param x           the column value
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void updateRowId(final String columnLabel, final java.sql.RowId x) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates are not supported");

    }

    /**
     * Retrieves the holdability of this <code>ResultSet</code> object
     *
     * @return either <code>ResultSet.HOLD_CURSORS_OVER_COMMIT</code> or <code>ResultSet.CLOSE_CURSORS_AT_COMMIT</code>
     * @throws java.sql.SQLException if a database access error occurs or this method is called on a closed result set
     * @since 1.6
     */
    public int getHoldability() throws SQLException {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    /**
     * Retrieves whether this <code>ResultSet</code> object has been closed. A <code>ResultSet</code> is closed if the
     * method close has been called on it, or if it is automatically closed.
     *
     * @return true if this <code>ResultSet</code> object is closed; false if it is still open
     * @throws java.sql.SQLException if a database access error occurs
     * @since 1.6
     */
    public boolean isClosed() throws SQLException {
        if (queryResult == null)
            return true;
        return queryResult.isClosed();
    }

    /**
     * Updates the designated column with a <code>String</code> value. It is intended for use when updating
     * <code>NCHAR</code>,<code>NVARCHAR</code> and <code>LONGNVARCHAR</code> columns. The updater methods are used to
     * update column values in the current row or the insert row.  The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code> methods are called to update the
     * database.
     *
     * @param columnIndex the first column is 1, the second 2, ...
     * @param nString     the value for the column to be updated
     * @throws java.sql.SQLException if the columnIndex is not valid; if the driver does not support national character
     *                               sets;  if the driver can detect that a data conversion error could occur; this
     *                               method is called on a closed result set; the result set concurrency is
     *                               <code>CONCUR_READ_ONLY</code> or if a database access error occurs
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void updateNString(final int columnIndex, final String nString) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * Updates the designated column with a <code>String</code> value. It is intended for use when updating
     * <code>NCHAR</code>,<code>NVARCHAR</code> and <code>LONGNVARCHAR</code> columns. The updater methods are used to
     * update column values in the current row or the insert row.  The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code> methods are called to update the
     * database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @param nString     the value for the column to be updated
     * @throws java.sql.SQLException if the columnLabel is not valid; if the driver does not support national character
     *                               sets;  if the driver can detect that a data conversion error could occur; this
     *                               method is called on a closed result set; the result set concurrency is
     *                               <CODE>CONCUR_READ_ONLY</code> or if a database access error occurs
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void updateNString(final String columnLabel, final String nString) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * Updates the designated column with a <code>java.sql.NClob</code> value. The updater methods are used to update
     * column values in the current row or the insert row.  The updater methods do not update the underlying database;
     * instead the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second 2, ...
     * @param nClob       the value for the column to be updated
     * @throws java.sql.SQLException if the columnIndex is not valid; if the driver does not support national character
     *                               sets;  if the driver can detect that a data conversion error could occur; this
     *                               method is called on a closed result set; if a database access error occurs or the
     *                               result set concurrency is <code>CONCUR_READ_ONLY</code>
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void updateNClob(final int columnIndex, final java.sql.NClob nClob) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * Updates the designated column with a <code>java.sql.NClob</code> value. The updater methods are used to update
     * column values in the current row or the insert row.  The updater methods do not update the underlying database;
     * instead the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @param nClob       the value for the column to be updated
     * @throws java.sql.SQLException if the columnLabel is not valid; if the driver does not support national character
     *                               sets;  if the driver can detect that a data conversion error could occur; this
     *                               method is called on a closed result set; if a database access error occurs or the
     *                               result set concurrency is <code>CONCUR_READ_ONLY</code>
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void updateNClob(final String columnLabel, final java.sql.NClob nClob) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates are not supported");
    }

    /**
     * Retrieves the value of the designated column in the current row of this <code>ResultSet</code> object as a
     * <code>NClob</code> object in the Java programming language.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return a <code>NClob</code> object representing the SQL <code>NCLOB</code> value in the specified column
     * @throws java.sql.SQLException if the columnIndex is not valid; if the driver does not support national character
     *                               sets;  if the driver can detect that a data conversion error could occur; this
     *                               method is called on a closed result set or if a database access error occurs
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public java.sql.NClob getNClob(final int columnIndex) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("NClobs are not supported");
    }

    /**
     * Retrieves the value of the designated column in the current row of this <code>ResultSet</code> object as a
     * <code>NClob</code> object in the Java programming language.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @return a <code>NClob</code> object representing the SQL <code>NCLOB</code> value in the specified column
     * @throws java.sql.SQLException if the columnLabel is not valid; if the driver does not support national character
     *                               sets;  if the driver can detect that a data conversion error could occur; this
     *                               method is called on a closed result set or if a database access error occurs
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public java.sql.NClob getNClob(final String columnLabel) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("NClobs are not supported");
    }

    /**
     * Retrieves the value of the designated column in  the current row of this <code>ResultSet</code> as a
     * <code>java.sql.SQLXML</code> object in the Java programming language.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return a <code>SQLXML</code> object that maps an <code>SQL XML</code> value
     * @throws java.sql.SQLException if the columnIndex is not valid; if a database access error occurs or this method
     *                               is called on a closed result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public java.sql.SQLXML getSQLXML(final int columnIndex) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("SQLXML not supported");
    }

    /**
     * Retrieves the value of the designated column in  the current row of this <code>ResultSet</code> as a
     * <code>java.sql.SQLXML</code> object in the Java programming language.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @return a <code>SQLXML</code> object that maps an <code>SQL XML</code> value
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs or this method
     *                               is called on a closed result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public java.sql.SQLXML getSQLXML(final String columnLabel) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("SQLXML not supported");
    }

    /**
     * Updates the designated column with a <code>java.sql.SQLXML</code> value. The updater methods are used to update
     * column values in the current row or the insert row. The updater methods do not update the underlying database;
     * instead the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     * <p/>
     *
     * @param columnIndex the first column is 1, the second 2, ...
     * @param xmlObject   the value for the column to be updated
     * @throws java.sql.SQLException if the columnIndex is not valid; if a database access error occurs; this method is
     *                               called on a closed result set; the <code>java.xml.transform.Result</code>,
     *                               <code>Writer</code> or <code>OutputStream</code> has not been closed for the
     *                               <code>SQLXML</code> object; if there is an error processing the XML value or the
     *                               result set concurrency is <code>CONCUR_READ_ONLY</code>.  The <code>getCause</code>
     *                               method of the exception may provide a more detailed exception, for example, if the
     *                               stream does not contain valid XML.
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void updateSQLXML(final int columnIndex, final java.sql.SQLXML xmlObject) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("SQLXML not supported");
    }

    /**
     * Updates the designated column with a <code>java.sql.SQLXML</code> value. The updater methods are used to update
     * column values in the current row or the insert row. The updater methods do not update the underlying database;
     * instead the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     * <p/>
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @param xmlObject   the column value
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs; this method is
     *                               called on a closed result set; the <code>java.xml.transform.Result</code>,
     *                               <code>Writer</code> or <code>OutputStream</code> has not been closed for the
     *                               <code>SQLXML</code> object; if there is an error processing the XML value or the
     *                               result set concurrency is <code>CONCUR_READ_ONLY</code>.  The <code>getCause</code>
     *                               method of the exception may provide a more detailed exception, for example, if the
     *                               stream does not contain valid XML.
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void updateSQLXML(final String columnLabel, final java.sql.SQLXML xmlObject) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("SQLXML not supported");
    }

    /**
     * Retrieves the value of the designated column in the current row of this <code>ResultSet</code> object as a
     * <code>String</code> in the Java programming language. It is intended for use when accessing
     * <code>NCHAR</code>,<code>NVARCHAR</code> and <code>LONGNVARCHAR</code> columns.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL <code>NULL</code>, the value returned is <code>null</code>
     * @throws java.sql.SQLException if the columnIndex is not valid; if a database access error occurs or this method
     *                               is called on a closed result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public String getNString(final int columnIndex) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("NString not supported");
    }

    /**
     * Retrieves the value of the designated column in the current row of this <code>ResultSet</code> object as a
     * <code>String</code> in the Java programming language. It is intended for use when accessing
     * <code>NCHAR</code>,<code>NVARCHAR</code> and <code>LONGNVARCHAR</code> columns.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @return the column value; if the value is SQL <code>NULL</code>, the value returned is <code>null</code>
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs or this method
     *                               is called on a closed result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public String getNString(final String columnLabel) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("NString not supported");
    }

    /**
     * Retrieves the value of the designated column in the current row of this <code>ResultSet</code> object as a
     * <code>java.io.Reader</code> object. It is intended for use when accessing  <code>NCHAR</code>,<code>NVARCHAR</code>
     * and <code>LONGNVARCHAR</code> columns.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return a <code>java.io.Reader</code> object that contains the column value; if the value is SQL
     *         <code>NULL</code>, the value returned is <code>null</code> in the Java programming language.
     * @throws java.sql.SQLException if the columnIndex is not valid; if a database access error occurs or this method
     *                               is called on a closed result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public Reader getNCharacterStream(final int columnIndex) throws SQLException {
        return getCharacterStream(columnIndex);
    }

    /**
     * Retrieves the value of the designated column in the current row of this <code>ResultSet</code> object as a
     * <code>java.io.Reader</code> object. It is intended for use when accessing  <code>NCHAR</code>,<code>NVARCHAR</code>
     * and <code>LONGNVARCHAR</code> columns.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @return a <code>java.io.Reader</code> object that contains the column value; if the value is SQL
     *         <code>NULL</code>, the value returned is <code>null</code> in the Java programming language
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs or this method
     *                               is called on a closed result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public Reader getNCharacterStream(final String columnLabel) throws SQLException {
        return getCharacterStream(columnLabel);
    }

    /**
     * Updates the designated column with a character stream value, which will have the specified number of bytes.   The
     * driver does the necessary conversion from Java character format to the national character set in the database. It
     * is intended for use when updating  <code>NCHAR</code>,<code>NVARCHAR</code> and <code>LONGNVARCHAR</code>
     * columns.
     * <p/>
     * The updater methods are used to update column values in the current row or the insert row.  The updater methods
     * do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @param length      the length of the stream
     * @throws java.sql.SQLException if the columnIndex is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void updateNCharacterStream(final int columnIndex, final Reader x, final long length) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * Updates the designated column with a character stream value, which will have the specified number of bytes.  The
     * driver does the necessary conversion from Java character format to the national character set in the database. It
     * is intended for use when updating  <code>NCHAR</code>,<code>NVARCHAR</code> and <code>LONGNVARCHAR</code>
     * columns.
     * <p/>
     * The updater methods are used to update column values in the current row or the insert row.  The updater methods
     * do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @param reader      the <code>java.io.Reader</code> object containing the new column value
     * @param length      the length of the stream
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void updateNCharacterStream(final String columnLabel, final Reader reader, final long length) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * Updates the designated column with an ascii stream value, which will have the specified number of bytes.
     * <p/>
     * The updater methods are used to update column values in the current row or the insert row.  The updater methods
     * do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @param length      the length of the stream
     * @throws java.sql.SQLException if the columnIndex is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void updateAsciiStream(final int columnIndex, final InputStream x, final long length) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * Updates the designated column with a binary stream value, which will have the specified number of bytes.
     * <p/>
     * The updater methods are used to update column values in the current row or the insert row.  The updater methods
     * do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @param length      the length of the stream
     * @throws java.sql.SQLException if the columnIndex is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void updateBinaryStream(final int columnIndex, final InputStream x, final long length) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * Updates the designated column with a character stream value, which will have the specified number of bytes.
     * <p/>
     * The updater methods are used to update column values in the current row or the insert row.  The updater methods
     * do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @param length      the length of the stream
     * @throws java.sql.SQLException if the columnIndex is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void updateCharacterStream(final int columnIndex, final Reader x, final long length) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * Updates the designated column with an ascii stream value, which will have the specified number of bytes.
     * <p/>
     * The updater methods are used to update column values in the current row or the insert row.  The updater methods
     * do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @param x           the new column value
     * @param length      the length of the stream
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void updateAsciiStream(final String columnLabel, final InputStream x, final long length) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * Updates the designated column with a binary stream value, which will have the specified number of bytes.
     * <p/>
     * The updater methods are used to update column values in the current row or the insert row.  The updater methods
     * do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @param x           the new column value
     * @param length      the length of the stream
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void updateBinaryStream(final String columnLabel, final InputStream x, final long length) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * Updates the designated column with a character stream value, which will have the specified number of bytes.
     * <p/>
     * The updater methods are used to update column values in the current row or the insert row.  The updater methods
     * do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @param reader      the <code>java.io.Reader</code> object containing the new column value
     * @param length      the length of the stream
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void updateCharacterStream(final String columnLabel, final Reader reader, final long length) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * Updates the designated column using the given input stream, which will have the specified number of bytes.
     * <p/>
     * <p/>
     * The updater methods are used to update column values in the current row or the insert row.  The updater methods
     * do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param inputStream An object that contains the data to set the parameter value to.
     * @param length      the number of bytes in the parameter data.
     * @throws java.sql.SQLException if the columnIndex is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void updateBlob(final int columnIndex, final InputStream inputStream, final long length) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * Updates the designated column using the given input stream, which will have the specified number of bytes.
     * <p/>
     * <p/>
     * The updater methods are used to update column values in the current row or the insert row.  The updater methods
     * do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @param inputStream An object that contains the data to set the parameter value to.
     * @param length      the number of bytes in the parameter data.
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void updateBlob(final String columnLabel, final InputStream inputStream, final long length) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * Updates the designated column using the given <code>Reader</code> object, which is the given number of characters
     * long. When a very large UNICODE value is input to a <code>LONGVARCHAR</code> parameter, it may be more practical
     * to send it via a <code>java.io.Reader</code> object. The JDBC driver will do any necessary conversion from
     * UNICODE to the database char format.
     * <p/>
     * <p/>
     * The updater methods are used to update column values in the current row or the insert row.  The updater methods
     * do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param reader      An object that contains the data to set the parameter value to.
     * @param length      the number of characters in the parameter data.
     * @throws java.sql.SQLException if the columnIndex is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void updateClob(final int columnIndex, final Reader reader, final long length) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * Updates the designated column using the given <code>Reader</code> object, which is the given number of characters
     * long. When a very large UNICODE value is input to a <code>LONGVARCHAR</code> parameter, it may be more practical
     * to send it via a <code>java.io.Reader</code> object.  The JDBC driver will do any necessary conversion from
     * UNICODE to the database char format.
     * <p/>
     * <p/>
     * The updater methods are used to update column values in the current row or the insert row.  The updater methods
     * do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @param reader      An object that contains the data to set the parameter value to.
     * @param length      the number of characters in the parameter data.
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void updateClob(final String columnLabel, final Reader reader, final long length) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * Updates the designated column using the given <code>Reader</code> object, which is the given number of characters
     * long. When a very large UNICODE value is input to a <code>LONGVARCHAR</code> parameter, it may be more practical
     * to send it via a <code>java.io.Reader</code> object. The JDBC driver will do any necessary conversion from
     * UNICODE to the database char format.
     * <p/>
     * <p/>
     * The updater methods are used to update column values in the current row or the insert row.  The updater methods
     * do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     *
     * @param columnIndex the first column is 1, the second 2, ...
     * @param reader      An object that contains the data to set the parameter value to.
     * @param length      the number of characters in the parameter data.
     * @throws java.sql.SQLException if the columnIndex is not valid; if the driver does not support national character
     *                               sets;  if the driver can detect that a data conversion error could occur; this
     *                               method is called on a closed result set, if a database access error occurs or the
     *                               result set concurrency is <code>CONCUR_READ_ONLY</code>
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void updateNClob(final int columnIndex, final Reader reader, final long length) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * Updates the designated column using the given <code>Reader</code> object, which is the given number of characters
     * long. When a very large UNICODE value is input to a <code>LONGVARCHAR</code> parameter, it may be more practical
     * to send it via a <code>java.io.Reader</code> object. The JDBC driver will do any necessary conversion from
     * UNICODE to the database char format.
     * <p/>
     * <p/>
     * The updater methods are used to update column values in the current row or the insert row.  The updater methods
     * do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @param reader      An object that contains the data to set the parameter value to.
     * @param length      the number of characters in the parameter data.
     * @throws java.sql.SQLException if the columnLabel is not valid; if the driver does not support national character
     *                               sets;  if the driver can detect that a data conversion error could occur; this
     *                               method is called on a closed result set; if a database access error occurs or the
     *                               result set concurrency is <code>CONCUR_READ_ONLY</code>
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void updateNClob(final String columnLabel, final Reader reader, final long length) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * Updates the designated column with a character stream value. The data will be read from the stream as needed
     * until end-of-stream is reached.  The driver does the necessary conversion from Java character format to the
     * national character set in the database. It is intended for use when updating
     * <code>NCHAR</code>,<code>NVARCHAR</code> and <code>LONGNVARCHAR</code> columns.
     * <p/>
     * The updater methods are used to update column values in the current row or the insert row.  The updater methods
     * do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     * <p/>
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if it might be more efficient to use a
     * version of <code>updateNCharacterStream</code> which takes a length parameter.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @throws java.sql.SQLException if the columnIndex is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void updateNCharacterStream(final int columnIndex, final Reader x) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * Updates the designated column with a character stream value. The data will be read from the stream as needed
     * until end-of-stream is reached.  The driver does the necessary conversion from Java character format to the
     * national character set in the database. It is intended for use when updating
     * <code>NCHAR</code>,<code>NVARCHAR</code> and <code>LONGNVARCHAR</code> columns.
     * <p/>
     * The updater methods are used to update column values in the current row or the insert row.  The updater methods
     * do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     * <p/>
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if it might be more efficient to use a
     * version of <code>updateNCharacterStream</code> which takes a length parameter.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @param reader      the <code>java.io.Reader</code> object containing the new column value
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void updateNCharacterStream(final String columnLabel, final Reader reader) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * Updates the designated column with an ascii stream value. The data will be read from the stream as needed until
     * end-of-stream is reached.
     * <p/>
     * The updater methods are used to update column values in the current row or the insert row.  The updater methods
     * do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     * <p/>
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if it might be more efficient to use a
     * version of <code>updateAsciiStream</code> which takes a length parameter.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @throws java.sql.SQLException if the columnIndex is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void updateAsciiStream(final int columnIndex, final InputStream x) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * Updates the designated column with a binary stream value. The data will be read from the stream as needed until
     * end-of-stream is reached.
     * <p/>
     * The updater methods are used to update column values in the current row or the insert row.  The updater methods
     * do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     * <p/>
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if it might be more efficient to use a
     * version of <code>updateBinaryStream</code> which takes a length parameter.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @throws java.sql.SQLException if the columnIndex is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void updateBinaryStream(final int columnIndex, final InputStream x) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * Updates the designated column with a character stream value. The data will be read from the stream as needed
     * until end-of-stream is reached.
     * <p/>
     * The updater methods are used to update column values in the current row or the insert row.  The updater methods
     * do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     * <p/>
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if it might be more efficient to use a
     * version of <code>updateCharacterStream</code> which takes a length parameter.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @throws java.sql.SQLException if the columnIndex is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void updateCharacterStream(final int columnIndex, final Reader x) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * Updates the designated column with an ascii stream value. The data will be read from the stream as needed until
     * end-of-stream is reached.
     * <p/>
     * The updater methods are used to update column values in the current row or the insert row.  The updater methods
     * do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     * <p/>
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if it might be more efficient to use a
     * version of <code>updateAsciiStream</code> which takes a length parameter.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @param x           the new column value
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void updateAsciiStream(final String columnLabel, final InputStream x) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * Updates the designated column with a binary stream value. The data will be read from the stream as needed until
     * end-of-stream is reached.
     * <p/>
     * The updater methods are used to update column values in the current row or the insert row.  The updater methods
     * do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     * <p/>
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if it might be more efficient to use a
     * version of <code>updateBinaryStream</code> which takes a length parameter.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @param x           the new column value
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void updateBinaryStream(final String columnLabel, final InputStream x) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * Updates the designated column with a character stream value. The data will be read from the stream as needed
     * until end-of-stream is reached.
     * <p/>
     * The updater methods are used to update column values in the current row or the insert row.  The updater methods
     * do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     * <p/>
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if it might be more efficient to use a
     * version of <code>updateCharacterStream</code> which takes a length parameter.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @param reader      the <code>java.io.Reader</code> object containing the new column value
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void updateCharacterStream(final String columnLabel, final Reader reader) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * Updates the designated column using the given input stream. The data will be read from the stream as needed until
     * end-of-stream is reached.
     * <p/>
     * The updater methods are used to update column values in the current row or the insert row.  The updater methods
     * do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     * <p/>
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if it might be more efficient to use a
     * version of <code>updateBlob</code> which takes a length parameter.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param inputStream An object that contains the data to set the parameter value to.
     * @throws java.sql.SQLException if the columnIndex is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void updateBlob(final int columnIndex, final InputStream inputStream) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * Updates the designated column using the given input stream. The data will be read from the stream as needed until
     * end-of-stream is reached.
     * <p/>
     * The updater methods are used to update column values in the current row or the insert row.  The updater methods
     * do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     * <p/>
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if it might be more efficient to use a
     * version of <code>updateBlob</code> which takes a length parameter.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @param inputStream An object that contains the data to set the parameter value to.
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void updateBlob(final String columnLabel, final InputStream inputStream) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * Updates the designated column using the given <code>Reader</code> object. The data will be read from the stream
     * as needed until end-of-stream is reached.  The JDBC driver will do any necessary conversion from UNICODE to the
     * database char format.
     * <p/>
     * <p/>
     * The updater methods are used to update column values in the current row or the insert row.  The updater methods
     * do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     * <p/>
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if it might be more efficient to use a
     * version of <code>updateClob</code> which takes a length parameter.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param reader      An object that contains the data to set the parameter value to.
     * @throws java.sql.SQLException if the columnIndex is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void updateClob(final int columnIndex, final Reader reader) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * Updates the designated column using the given <code>Reader</code> object. The data will be read from the stream
     * as needed until end-of-stream is reached.  The JDBC driver will do any necessary conversion from UNICODE to the
     * database char format.
     * <p/>
     * <p/>
     * The updater methods are used to update column values in the current row or the insert row.  The updater methods
     * do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     * <p/>
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if it might be more efficient to use a
     * version of <code>updateClob</code> which takes a length parameter.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @param reader      An object that contains the data to set the parameter value to.
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs; the result set
     *                               concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed
     *                               result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void updateClob(final String columnLabel, final Reader reader) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * Updates the designated column using the given <code>Reader</code>
     * <p/>
     * The data will be read from the stream as needed until end-of-stream is reached.  The JDBC driver will do any
     * necessary conversion from UNICODE to the database char format.
     * <p/>
     * <p/>
     * The updater methods are used to update column values in the current row or the insert row.  The updater methods
     * do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     * <p/>
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if it might be more efficient to use a
     * version of <code>updateNClob</code> which takes a length parameter.
     *
     * @param columnIndex the first column is 1, the second 2, ...
     * @param reader      An object that contains the data to set the parameter value to.
     * @throws java.sql.SQLException if the columnIndex is not valid; if the driver does not support national character
     *                               sets;  if the driver can detect that a data conversion error could occur; this
     *                               method is called on a closed result set, if a database access error occurs or the
     *                               result set concurrency is <code>CONCUR_READ_ONLY</code>
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void updateNClob(final int columnIndex, final Reader reader) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    /**
     * Updates the designated column using the given <code>Reader</code> object. The data will be read from the stream
     * as needed until end-of-stream is reached.  The JDBC driver will do any necessary conversion from UNICODE to the
     * database char format.
     * <p/>
     * <p/>
     * The updater methods are used to update column values in the current row or the insert row.  The updater methods
     * do not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     * <p/>
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if it might be more efficient to use a
     * version of <code>updateNClob</code> which takes a length parameter.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @param reader      An object that contains the data to set the parameter value to.
     * @throws java.sql.SQLException if the columnLabel is not valid; if the driver does not support national character
     *                               sets;  if the driver can detect that a data conversion error could occur; this
     *                               method is called on a closed result set; if a database access error occurs or the
     *                               result set concurrency is <code>CONCUR_READ_ONLY</code>
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void updateNClob(final String columnLabel, final Reader reader) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Updates not supported");
    }

    public boolean getBoolean(final int i) throws SQLException {
        return getValueObject(i).getBoolean();
    }

    public byte getByte(final int i) throws SQLException {
        return getValueObject(i).getByte();
    }

    public short getShort(final int i) throws SQLException {
        return getValueObject(i).getShort();
    }

    public long getLong(final int i) throws SQLException {
        return getValueObject(i).getLong();
    }

    public float getFloat(final int i) throws SQLException {
        return getValueObject(i).getFloat();
    }

    public double getDouble(final int i) throws SQLException {
        return getValueObject(i).getDouble();
    }

    /**
     * Retrieves the value of the designated column in the current row of this <code>ResultSet</code> object as a
     * <code>java.sql.BigDecimal</code> in the Java programming language.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param scale       the number of digits to the right of the decimal point
     * @return the column value; if the value is SQL <code>NULL</code>, the value returned is <code>null</code>
     * @throws java.sql.SQLException if the columnIndex is not valid; if a database access error occurs or this method
     *                               is called on a closed result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @deprecated
     */
    public BigDecimal getBigDecimal(final int columnIndex, final int scale) throws SQLException {
        return getValueObject(columnIndex).getBigDecimal();
    }

    /**
     * Retrieves the value of the designated column in the current row of this <code>ResultSet</code> object as a
     * <code>byte</code> array in the Java programming language. The bytes represent the raw values returned by the
     * driver.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL <code>NULL</code>, the value returned is <code>null</code>
     * @throws java.sql.SQLException if the columnIndex is not valid; if a database access error occurs or this method
     *                               is called on a closed result set
     */
    public byte[] getBytes(final int columnIndex) throws SQLException {
        return getValueObject(columnIndex).getBytes();
    }

    /**
     * Retrieves the value of the designated column in the current row of this <code>ResultSet</code> object as a
     * <code>java.sql.Date</code> object in the Java programming language.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL <code>NULL</code>, the value returned is <code>null</code>
     * @throws java.sql.SQLException if the columnIndex is not valid; if a database access error occurs or this method
     *                               is called on a closed result set
     */
    public Date getDate(final int columnIndex) throws SQLException {
        try {
            return getValueObject(columnIndex).getDate();
        } catch (ParseException e) {
            throw SQLExceptionMapper.getSQLException("Could not parse field as date", e);
        }
    }

    /**
     * Retrieves the value of the designated column in the current row of this <code>ResultSet</code> object as a
     * <code>java.sql.Time</code> object in the Java programming language.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL <code>NULL</code>, the value returned is <code>null</code>
     * @throws java.sql.SQLException if the columnIndex is not valid; if a database access error occurs or this method
     *                               is called on a closed result set
     */
    public Time getTime(final int columnIndex) throws SQLException {
        try {
            return getValueObject(columnIndex).getTime();
        } catch (ParseException e) {
            throw SQLExceptionMapper.getSQLException("Could not parse column as time, was: \"" +
                    getValueObject(columnIndex).getString() +
                    "\"", e);
        }
    }

    /**
     * Retrieves the value of the designated column in the current row of this <code>ResultSet</code> object as a
     * <code>java.sql.Timestamp</code> object in the Java programming language.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL <code>NULL</code>, the value returned is <code>null</code>
     * @throws java.sql.SQLException if the columnIndex is not valid; if a database access error occurs or this method
     *                               is called on a closed result set
     */
    public Timestamp getTimestamp(final int columnIndex) throws SQLException {
        try {
            return getValueObject(columnIndex).getTimestamp();
        } catch (ParseException e) {
            throw SQLExceptionMapper.getSQLException("Could not parse field as date");
        }
    }

    /**
     * Retrieves the value of the designated column in the current row of this <code>ResultSet</code> object as a stream
     * of ASCII characters. The value can then be read in chunks from the stream. This method is particularly suitable
     * for retrieving large <char>LONGVARCHAR</char> values. The JDBC driver will do any necessary conversion from the
     * database format into ASCII.
     * <p/>
     * <P><B>Note:</B> All the data in the returned stream must be read prior to getting the value of any other column.
     * The next call to a getter method implicitly closes the stream.  Also, a stream may return <code>0</code> when the
     * method <code>InputStream.available</code> is called whether there is data available or not.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return a Java input stream that delivers the database column value as a stream of one-byte ASCII characters; if
     *         the value is SQL <code>NULL</code>, the value returned is <code>null</code>
     * @throws java.sql.SQLException if the columnIndex is not valid; if a database access error occurs or this method
     *                               is called on a closed result set
     */
    public InputStream getAsciiStream(final int columnIndex) throws SQLException {
        return getValueObject(columnIndex).getInputStream();
    }

    /**
     * Retrieves the value of the designated column in the current row of this <code>ResultSet</code> object as as a
     * stream of two-byte 3 characters. The first byte is the high byte; the second byte is the low byte.
     * <p/>
     * The value can then be read in chunks from the stream. This method is particularly suitable for retrieving large
     * <code>LONGVARCHAR</code>values.  The JDBC driver will do any necessary conversion from the database format into
     * Unicode.
     * <p/>
     * <P><B>Note:</B> All the data in the returned stream must be read prior to getting the value of any other column.
     * The next call to a getter method implicitly closes the stream. Also, a stream may return <code>0</code> when the
     * method <code>InputStream.available</code> is called, whether there is data available or not.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return a Java input stream that delivers the database column value as a stream of two-byte Unicode characters;
     *         if the value is SQL <code>NULL</code>, the value returned is <code>null</code>
     * @throws java.sql.SQLException if the columnIndex is not valid; if a database access error occurs or this method
     *                               is called on a closed result set
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @deprecated use <code>getCharacterStream</code> in place of <code>getUnicodeStream</code>
     */
    public InputStream getUnicodeStream(final int columnIndex) throws SQLException {
        return getValueObject(columnIndex).getInputStream();
    }

    /**
     * Retrieves the value of the designated column in the current row of this <code>ResultSet</code> object as a stream
     * of uninterpreted bytes. The value can then be read in chunks from the stream. This method is particularly
     * suitable for retrieving large <code>LONGVARBINARY</code> values.
     * <p/>
     * <P><B>Note:</B> All the data in the returned stream must be read prior to getting the value of any other column.
     * The next call to a getter method implicitly closes the stream.  Also, a stream may return <code>0</code> when the
     * method <code>InputStream.available</code> is called whether there is data available or not.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return a Java input stream that delivers the database column value as a stream of uninterpreted bytes; if the
     *         value is SQL <code>NULL</code>, the value returned is <code>null</code>
     * @throws java.sql.SQLException if the columnIndex is not valid; if a database access error occurs or this method
     *                               is called on a closed result set
     */
    public InputStream getBinaryStream(final int columnIndex) throws SQLException {
        if(protocol.supportsPBMS()) {
            try {
                return getValueObject(columnIndex).getPBMSStream(protocol);
            } catch (QueryException e) {
                throw SQLExceptionMapper.get(e);
            } catch (IOException e) {
                throw SQLExceptionMapper.getSQLException("Could not read back the data using http", e);
            }
        }
        return getValueObject(columnIndex).getBinaryInputStream();

    }

    /**
     * Retrieves the value of the designated column in the current row of this <code>ResultSet</code> object as a
     * <code>String</code> in the Java programming language.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @return the column value; if the value is SQL <code>NULL</code>, the value returned is <code>null</code>
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs or this method
     *                               is called on a closed result set
     */
    public String getString(final String columnLabel) throws SQLException {
        final ValueObject vo = getValueObject(columnLabel);
        return vo.getString();
    }

    /**
     * Retrieves the value of the designated column in the current row of this <code>ResultSet</code> object as a
     * <code>boolean</code> in the Java programming language.
     * <p/>
     * <P>If the designated column has a datatype of CHAR or VARCHAR and contains a "0" or has a datatype of BIT,
     * TINYINT, SMALLINT, INTEGER or BIGINT and contains  a 0, a value of <code>false</code> is returned.  If the
     * designated column has a datatype of CHAR or VARCHAR and contains a "1" or has a datatype of BIT, TINYINT,
     * SMALLINT, INTEGER or BIGINT and contains  a 1, a value of <code>true</code> is returned.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @return the column value; if the value is SQL <code>NULL</code>, the value returned is <code>false</code>
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs or this method
     *                               is called on a closed result set
     */
    public boolean getBoolean(final String columnLabel) throws SQLException {
        return getValueObject(columnLabel).getBoolean();
    }

    /**
     * Retrieves the value of the designated column in the current row of this <code>ResultSet</code> object as a
     * <code>byte</code> in the Java programming language.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @return the column value; if the value is SQL <code>NULL</code>, the value returned is <code>0</code>
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs or this method
     *                               is called on a closed result set
     */
    public byte getByte(final String columnLabel) throws SQLException {
        return getValueObject(columnLabel).getByte();
    }

    /**
     * Retrieves the value of the designated column in the current row of this <code>ResultSet</code> object as a
     * <code>short</code> in the Java programming language.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not
     *                    specified, then the label is the name of the column
     * @return the column value; if the value is SQL <code>NULL</code>, the value returned is <code>0</code>
     * @throws java.sql.SQLException if the columnLabel is not valid; if a database access error occurs or this method
     *                               is called on a closed result set
     */
    public short getShort(final String columnLabel) throws SQLException {
        return getValueObject(columnLabel).getShort();
    }


    /**
     * Returns an object that implements the given interface to allow access to non-standard methods, or standard
     * methods not exposed by the proxy.
     * <p/>
     * If the receiver implements the interface then the result is the receiver or a proxy for the receiver. If the
     * receiver is a wrapper and the wrapped object implements the interface then the result is the wrapped object or a
     * proxy for the wrapped object. Otherwise return the the result of calling <code>unwrap</code> recursively on the
     * wrapped object or a proxy for that result. If the receiver is not a wrapper and does not implement the interface,
     * then an <code>SQLException</code> is thrown.
     *
     * @param iface A Class defining an interface that the result must implement.
     * @return an object that implements the interface. May be a proxy for the actual implementing object.
     * @throws java.sql.SQLException If no object found that implements the interface
     * @since 1.6
     */
    public <T> T unwrap(final Class<T> iface) throws SQLException {
        return null;
    }

    /**
     * Returns true if this either implements the interface argument or is directly or indirectly a wrapper for an
     * object that does. Returns false otherwise. If this implements the interface then return true, else if this is a
     * wrapper then return the result of recursively calling <code>isWrapperFor</code> on the wrapped object. If this
     * does not implement the interface and is not a wrapper, return false. This method should be implemented as a
     * low-cost operation compared to <code>unwrap</code> so that callers can use this method to avoid expensive
     * <code>unwrap</code> calls that may fail. If this method returns true then calling <code>unwrap</code> with the
     * same argument should succeed.
     *
     * @param iface a Class defining an interface.
     * @return true if this implements the interface or directly or indirectly wraps an object that does.
     * @throws java.sql.SQLException if an error occurs while determining whether this is a wrapper for an object with
     *                               the given interface.
     * @since 1.6
     */
    public boolean isWrapperFor(final Class<?> iface) throws SQLException {
        return false;
    }
}
