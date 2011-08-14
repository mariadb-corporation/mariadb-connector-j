/*
 * Drizzle-JDBC
 *
 * Copyright (c) 2009-2011, Marcus Eriksson, Trond Norbye, Stephane Giron
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
import org.skysql.jdbc.internal.common.ParameterizedBatchHandler;
import org.skysql.jdbc.internal.common.Protocol;
import org.skysql.jdbc.internal.common.QueryException;
import org.skysql.jdbc.internal.common.Utils;
import org.skysql.jdbc.internal.common.query.IllegalParameterException;
import org.skysql.jdbc.internal.common.query.ParameterizedQuery;
import org.skysql.jdbc.internal.common.query.QueryFactory;
import org.skysql.jdbc.internal.common.query.parameters.*;
import org.skysql.jdbc.internal.common.queryresults.ModifyQueryResult;
import org.skysql.jdbc.internal.common.queryresults.ResultSetType;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * User: marcuse Date: Jan 27, 2009 Time: 10:49:42 PM
 */
public class MySQLPreparedStatement extends MySQLStatement implements PreparedStatement {
    private final static Logger log = Logger.getLogger(MySQLPreparedStatement.class.getName());
    private ParameterizedQuery dQuery;
    private final ParameterizedBatchHandler parameterizedBatchHandler;

    public MySQLPreparedStatement(final Protocol protocol,
                                  final MySQLConnection connection,
                                  final String query,
                                  final QueryFactory queryFactory,
                                  final ParameterizedBatchHandler parameterizedBatchHandler) throws SQLException {
        super(protocol, connection, queryFactory);

        if(log.isLoggable(Level.FINEST)) {
            log.finest("Creating prepared statement for " + query);
        }
        dQuery = queryFactory.createParameterizedQuery(Utils.nativeSQL(query));
        this.parameterizedBatchHandler = parameterizedBatchHandler;
    }

    public ResultSet executeQuery() throws SQLException {
        executeQueryProlog();
        try {
            setQueryResult(getProtocol().executeQuery(dQuery, isStreaming()));
            return new MySQLResultSet(getQueryResult(), this, getProtocol());
        } catch (QueryException e) {
            throw SQLExceptionMapper.get(e);
        } finally {
            stopTimer();
        }
    }

    /**
     * Executes the SQL statement in this <code>PreparedStatement</code> object, which must be an SQL Data Manipulation
     * Language (DML) statement, such as <code>INSERT</code>, <code>UPDATE</code> or <code>DELETE</code>; or an SQL
     * statement that returns nothing, such as a DDL statement.
     *
     * @return either (1) the row count for SQL Data Manipulation Language (DML) statements or (2) 0 for SQL statements
     *         that return nothing
     * @throws java.sql.SQLException if a database access error occurs; this method is called on a closed
     *                               <code>PreparedStatement</code> or the SQL statement returns a
     *                               <code>ResultSet</code> object
     */
    public int executeUpdate() throws SQLException {
        executeQueryProlog();
        try {
            setQueryResult(getProtocol().executeQuery(dQuery));
            dQuery.clearParameters();
        } catch (QueryException e) {
            throw SQLExceptionMapper.get(e);
        } finally {
            stopTimer();
        }
        if (getQueryResult().getResultSetType() != ResultSetType.MODIFY) {
            throw SQLExceptionMapper.getSQLException("The query returned a result set");
        }
        return (int) ((ModifyQueryResult) getQueryResult()).getUpdateCount();
    }


    /**
     * Sets the designated parameter to SQL <code>NULL</code>.
     * <p/>
     * <P><B>Note:</B> You must specify the parameter's SQL type.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param sqlType        the SQL type code defined in <code>java.sql.Types</code>
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                               if a database access error occurs or this method is called on a closed
     *                               <code>PreparedStatement</code>
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if <code>sqlType</code> is a <code>ARRAY</code>, <code>BLOB</code>,
     *                               <code>CLOB</code>, <code>DATALINK</code>, <code>JAVA_OBJECT</code>,
     *                               <code>NCHAR</code>, <code>NCLOB</code>, <code>NVARCHAR</code>,
     *                               <code>LONGNVARCHAR</code>, <code>REF</code>, <code>ROWID</code>,
     *                               <code>SQLXML</code> or  <code>STRUCT</code> data type and the JDBC driver does not
     *                               support this data type
     */
    public void setNull(final int parameterIndex, final int sqlType) throws SQLException {
        setParameter(parameterIndex, new NullParameter());
    }

    public boolean execute() throws SQLException {
        executeQueryProlog();
        try {
            setQueryResult(getProtocol().executeQuery(dQuery, isStreaming()));
            dQuery.clearParameters();
        } catch (QueryException e) {
            throw SQLExceptionMapper.get(e);
        } finally {
            stopTimer();
        }
        if (getQueryResult().getResultSetType() == ResultSetType.SELECT) {
            //setResultSet(new MySQLResultSet(getQueryResult(), this, getProtocol()));
            return true;
        } else {
            setUpdateCount(((ModifyQueryResult) getQueryResult()).getUpdateCount());
            return false;
        }
    }

    /**
     * Adds a set of parameters to this <code>PreparedStatement</code> object's batch of commands.
     * <p/>
     * <p/>
     *
     * @throws java.sql.SQLException if a database access error occurs or this method is called on a closed
     *                               <code>PreparedStatement</code>
     * @see java.sql.Statement#addBatch
     * @since 1.2
     */
    public void addBatch() throws SQLException {
        parameterizedBatchHandler.addToBatch(dQuery);
        dQuery = getQueryFactory().createParameterizedQuery(dQuery);
    }
    public void addBatch(final String sql) throws SQLException {
        parameterizedBatchHandler.addToBatch(getQueryFactory().createParameterizedQuery(sql));
    }
    @Override
    public int[] executeBatch() throws SQLException {
        try {
            return parameterizedBatchHandler.executeBatch();
        } catch (QueryException e) {
            throw SQLExceptionMapper.get(e);
        }
    }


    /**
     * Sets the designated parameter to the given <code>Reader</code> object, which is the given number of characters
     * long. When a very large UNICODE value is input to a <code>LONGVARCHAR</code> parameter, it may be more practical
     * to send it via a <code>java.io.Reader</code> object. The data will be read from the stream as needed until
     * end-of-file is reached.  The JDBC driver will do any necessary conversion from UNICODE to the database char
     * format.
     * <p/>
     * <P><B>Note:</B> This stream object can either be a standard Java stream object or your own subclass that
     * implements the standard interface.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param reader         the <code>java.io.Reader</code> object that contains the Unicode data
     * @param length         the number of characters in the stream
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                               if a database access error occurs or this method is called on a closed
     *                               <code>PreparedStatement</code>
     * @since 1.2
     */
    public void setCharacterStream(final int parameterIndex, final Reader reader, final int length) throws SQLException {
        try {
            setParameter(parameterIndex, new ReaderParameter(reader, length));
        } catch (IOException e) {
            throw SQLExceptionMapper.getSQLException("Could not read stream: " + e.getMessage(), e);

        }
    }

    /**
     * Sets the designated parameter to the given <code>REF(&lt;structured-type&gt;)</code> value. The driver converts
     * this to an SQL <code>REF</code> value when it sends it to the database.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x              an SQL <code>REF</code> value
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                               if a database access error occurs or this method is called on a closed
     *                               <code>PreparedStatement</code>
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public void setRef(final int parameterIndex, final Ref x) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("REF not supported");
    }

    /**
     * Sets the designated parameter to the given <code>java.sql.Blob</code> object. The driver converts this to an SQL
     * <code>BLOB</code> value when it sends it to the database.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x              a <code>Blob</code> object that maps an SQL <code>BLOB</code> value
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                               if a database access error occurs or this method is called on a closed
     *                               <code>PreparedStatement</code>
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public void setBlob(final int parameterIndex, final Blob x) throws SQLException {
        if(x == null) {
            setNull(parameterIndex, Types.BLOB);
            return;
        }
        try {
            setParameter(parameterIndex, new StreamParameter(x.getBinaryStream(), x.length()));
        } catch (IOException e) {
            throw SQLExceptionMapper.getSQLException("Could not read stream", e);
        }
    }

    /**
     * Sets the designated parameter to the given <code>java.sql.Clob</code> object. The driver converts this to an SQL
     * <code>CLOB</code> value when it sends it to the database.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x              a <code>Clob</code> object that maps an SQL <code>CLOB</code> value
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                               if a database access error occurs or this method is called on a closed
     *                               <code>PreparedStatement</code>
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public void setClob(final int parameterIndex, final Clob x) throws SQLException {
        if(x == null) {
            setNull(parameterIndex, Types.CLOB);
            return;
        }
        try {
            setParameter(parameterIndex, new StreamParameter(x.getAsciiStream(), ((MySQLBlob)x).length()));
        } catch (IOException e) {
            throw SQLExceptionMapper.getSQLException("Could not read stream", e);
        }
    }

    /**
     * Sets the designated parameter to the given <code>java.sql.Array</code> object. The driver converts this to an SQL
     * <code>ARRAY</code> value when it sends it to the database.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x              an <code>Array</code> object that maps an SQL <code>ARRAY</code> value
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                               if a database access error occurs or this method is called on a closed
     *                               <code>PreparedStatement</code>
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public void setArray(final int parameterIndex, final Array x) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Arrays not supported");
    }

    /**
     * Retrieves a <code>ResultSetMetaData</code> object that contains information about the columns of the
     * <code>ResultSet</code> object that will be returned when this <code>PreparedStatement</code> object is executed.
     * <p/>
     * Because a <code>PreparedStatement</code> object is precompiled, it is possible to know about the
     * <code>ResultSet</code> object that it will return without having to execute it.  Consequently, it is possible to
     * invoke the method <code>getMetaData</code> on a <code>PreparedStatement</code> object rather than waiting to
     * execute it and then invoking the <code>ResultSet.getMetaData</code> method on the <code>ResultSet</code> object
     * that is returned.
     * <p/>
     * <B>NOTE:</B> Using this method may be expensive for some drivers due to the lack of underlying DBMS support.
     *
     * 
     * @return the description of a <code>ResultSet</code> object's columns or <code>null</code> if the driver cannot
     *         return a <code>ResultSetMetaData</code> object
     * @throws java.sql.SQLException if a database access error occurs or this method is called on a closed
     *                               <code>PreparedStatement</code>
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public ResultSetMetaData getMetaData() throws SQLException {
        if (super.getResultSet() != null) {
            return super.getResultSet().getMetaData();
        }
        return null;
    }

    /**
     * Sets the designated parameter to the given <code>java.sql.Date</code> value, using the given
     * <code>Calendar</code> object.  The driver uses the <code>Calendar</code> object to construct an SQL
     * <code>DATE</code> value, which the driver then sends to the database.  With a <code>Calendar</code> object, the
     * driver can calculate the date taking into account a custom timezone.  If no <code>Calendar</code> object is
     * specified, the driver uses the default timezone, which is that of the virtual machine running the application.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param date           the parameter value
     * @param cal            the <code>Calendar</code> object the driver will use to construct the date
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                               if a database access error occurs or this method is called on a closed
     *                               <code>PreparedStatement</code>
     * @since 1.2
     */
    public void setDate(final int parameterIndex, final Date date, final Calendar cal) throws SQLException {
        if(date == null) {
            setNull(parameterIndex, Types.DATE);
            return;
        }
        setParameter(parameterIndex, new DateParameter(date.getTime(), cal));
    }

    /**
     * Sets the designated parameter to the given <code>java.sql.Time</code> value, using the given
     * <code>Calendar</code> object.  The driver uses the <code>Calendar</code> object to construct an SQL
     * <code>TIME</code> value, which the driver then sends to the database.  With a <code>Calendar</code> object, the
     * driver can calculate the time taking into account a custom timezone.  If no <code>Calendar</code> object is
     * specified, the driver uses the default timezone, which is that of the virtual machine running the application.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param time           the parameter value
     * @param cal            the <code>Calendar</code> object the driver will use to construct the time
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                               if a database access error occurs or this method is called on a closed
     *                               <code>PreparedStatement</code>
     * @since 1.2
     */
    public void setTime(final int parameterIndex, final Time time, final Calendar cal) throws SQLException {
//        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
//        sdf.setCalendar(cal);
        if(time == null) {
            setNull(parameterIndex, Types.TIME);
            return;
        }


        setParameter(parameterIndex, new TimeParameter(time.getTime()));
    }

    /**
     * Sets the designated parameter to the given <code>java.sql.Timestamp</code> value, using the given
     * <code>Calendar</code> object.  The driver uses the <code>Calendar</code> object to construct an SQL
     * <code>TIMESTAMP</code> value, which the driver then sends to the database.  With a <code>Calendar</code> object,
     * the driver can calculate the timestamp taking into account a custom timezone.  If no <code>Calendar</code> object
     * is specified, the driver uses the default timezone, which is that of the virtual machine running the
     * application.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x              the parameter value
     * @param cal            the <code>Calendar</code> object the driver will use to construct the timestamp
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                               if a database access error occurs or this method is called on a closed
     *                               <code>PreparedStatement</code>
     * @since 1.2
     */
    public void setTimestamp(final int parameterIndex, final Timestamp x, final Calendar cal) throws SQLException {
        if(x == null) {
            setNull(parameterIndex, Types.TIMESTAMP);
            return;
        }

        setParameter(parameterIndex, new TimestampParameter(x.getTime(), cal));
    }

    /**
     * Sets the designated parameter to SQL <code>NULL</code>. This version of the method <code>setNull</code> should be
     * used for user-defined types and REF type parameters.  Examples of user-defined types include: STRUCT, DISTINCT,
     * JAVA_OBJECT, and named array types.
     * <p/>
     * <P><B>Note:</B> To be portable, applications must give the SQL type code and the fully-qualified SQL type name
     * when specifying a NULL user-defined or REF parameter.  In the case of a user-defined type the name is the type
     * name of the parameter itself.  For a REF parameter, the name is the type name of the referenced type.  If a JDBC
     * driver does not need the type code or type name information, it may ignore it.
     * <p/>
     * Although it is intended for user-defined and Ref parameters, this method may be used to set a null parameter of
     * any JDBC type. If the parameter does not have a user-defined or REF type, the given typeName is ignored.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param sqlType        a value from <code>java.sql.Types</code>
     * @param typeName       the fully-qualified name of an SQL user-defined type; ignored if the parameter is not a
     *                       user-defined type or REF
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                               if a database access error occurs or this method is called on a closed
     *                               <code>PreparedStatement</code>
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if <code>sqlType</code> is a <code>ARRAY</code>, <code>BLOB</code>,
     *                               <code>CLOB</code>, <code>DATALINK</code>, <code>JAVA_OBJECT</code>,
     *                               <code>NCHAR</code>, <code>NCLOB</code>, <code>NVARCHAR</code>,
     *                               <code>LONGNVARCHAR</code>, <code>REF</code>, <code>ROWID</code>,
     *                               <code>SQLXML</code> or  <code>STRUCT</code> data type and the JDBC driver does not
     *                               support this data type or if the JDBC driver does not support this method
     * @since 1.2
     */
    public void setNull(final int parameterIndex, final int sqlType, final String typeName) throws SQLException {
        setParameter(parameterIndex, new NullParameter());
    }

    private void setParameter(final int parameterIndex, final ParameterHolder holder) throws SQLException {
        try {
            dQuery.setParameter(parameterIndex - 1, holder);
        } catch (IllegalParameterException e) {
            throw SQLExceptionMapper.getSQLException("Could not set parameter", e);
        }
    }

    /**
     * Sets the designated parameter to the given <code>java.net.URL</code> value. The driver converts this to an SQL
     * <code>DATALINK</code> value when it sends it to the database.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x              the <code>java.net.URL</code> object to be set
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                               if a database access error occurs or this method is called on a closed
     *                               <code>PreparedStatement</code>
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.4
     */
    public void setURL(final int parameterIndex, final URL x) throws SQLException {
        setParameter(parameterIndex, new StringParameter(x.toString()));
    }

    /**
     * Retrieves the number, types and properties of this <code>PreparedStatement</code> object's parameters.
     *
     * @return a <code>ParameterMetaData</code> object that contains information about the number, types and properties
     *         for each parameter marker of this <code>PreparedStatement</code> object
     * @throws java.sql.SQLException if a database access error occurs or this method is called on a closed
     *                               <code>PreparedStatement</code>
     * @see java.sql.ParameterMetaData
     * @since 1.4
     */
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return new MySQLParameterMetaData(dQuery);
    }

    /**
     * Sets the designated parameter to the given <code>java.sql.RowId</code> object. The driver converts this to a SQL
     * <code>ROWID</code> value when it sends it to the database
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x              the parameter value
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                               if a database access error occurs or this method is called on a closed
     *                               <code>PreparedStatement</code>
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void setRowId(final int parameterIndex, final java.sql.RowId x) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("RowIDs not supported");
    }

    /**
     * Sets the designated paramter to the given <code>String</code> object. The driver converts this to a SQL
     * <code>NCHAR</code> or <code>NVARCHAR</code> or <code>LONGNVARCHAR</code> value (depending on the argument's size
     * relative to the driver's limits on <code>NVARCHAR</code> values) when it sends it to the database.
     *
     * @param parameterIndex of the first parameter is 1, the second is 2, ...
     * @param value          the parameter value
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                               if the driver does not support national character sets;  if the driver can detect
     *                               that a data conversion error could occur; if a database access error occurs; or
     *                               this method is called on a closed <code>PreparedStatement</code>
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void setNString(final int parameterIndex, final String value) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("NStrings not supported");
    }

    /**
     * Sets the designated parameter to a <code>Reader</code> object. The <code>Reader</code> reads the data till
     * end-of-file is reached. The driver does the necessary conversion from Java character format to the national
     * character set in the database.
     *
     * @param parameterIndex of the first parameter is 1, the second is 2, ...
     * @param value          the parameter value
     * @param length         the number of characters in the parameter data.
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                               if the driver does not support national character sets;  if the driver can detect
     *                               that a data conversion error could occur; if a database access error occurs; or
     *                               this method is called on a closed <code>PreparedStatement</code>
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void setNCharacterStream(final int parameterIndex, final Reader value, final long length) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("NCharstreams not supported");
    }

    /**
     * Sets the designated parameter to a <code>java.sql.NClob</code> object. The driver converts this to a SQL
     * <code>NCLOB</code> value when it sends it to the database.
     *
     * @param parameterIndex of the first parameter is 1, the second is 2, ...
     * @param value          the parameter value
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                               if the driver does not support national character sets;  if the driver can detect
     *                               that a data conversion error could occur; if a database access error occurs; or
     *                               this method is called on a closed <code>PreparedStatement</code>
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void setNClob(final int parameterIndex, final java.sql.NClob value) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("NClobs not supported");
    }

    /**
     * Sets the designated parameter to a <code>Reader</code> object.  The reader must contain  the number of characters
     * specified by length otherwise a <code>SQLException</code> will be generated when the
     * <code>PreparedStatement</code> is executed. This method differs from the <code>setCharacterStream (int, Reader,
     * int)</code> method because it informs the driver that the parameter value should be sent to the server as a
     * <code>CLOB</code>.  When the <code>setCharacterStream</code> method is used, the driver may have to do extra work
     * to determine whether the parameter data should be sent to the server as a <code>LONGVARCHAR</code> or a
     * <code>CLOB</code>
     *
     * @param parameterIndex index of the first parameter is 1, the second is 2, ...
     * @param reader         An object that contains the data to set the parameter value to.
     * @param length         the number of characters in the parameter data.
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                               if a database access error occurs; this method is called on a closed
     *                               <code>PreparedStatement</code> or if the length specified is less than zero.
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void setClob(final int parameterIndex, final Reader reader, final long length) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("Clobs not supported");
    }

    /**
     * Sets the designated parameter to a <code>InputStream</code> object.  The inputstream must contain  the number of
     * characters specified by length otherwise a <code>SQLException</code> will be generated when the
     * <code>PreparedStatement</code> is executed. This method differs from the <code>setBinaryStream (int, InputStream,
     * int)</code> method because it informs the driver that the parameter value should be sent to the server as a
     * <code>BLOB</code>.  When the <code>setBinaryStream</code> method is used, the driver may have to do extra work to
     * determine whether the parameter data should be sent to the server as a <code>LONGVARBINARY</code> or a
     * <code>BLOB</code>
     *
     * @param parameterIndex index of the first parameter is 1, the second is 2, ...
     * @param inputStream    An object that contains the data to set the parameter value to.
     * @param length         the number of bytes in the parameter data.
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                               if a database access error occurs; this method is called on a closed
     *                               <code>PreparedStatement</code>; if the length specified is less than zero or if the
     *                               number of bytes in the inputstream does not match the specfied length.
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void setBlob(final int parameterIndex, final InputStream inputStream, final long length) throws SQLException {
        if(inputStream == null) {
            setNull(parameterIndex, Types.BLOB);
            return;
        }

        try {
            setParameter(parameterIndex, new StreamParameter(inputStream, length));
        } catch (IOException e) {
            throw SQLExceptionMapper.getSQLException("Could not read stream", e);
        }
    }

    /**
     * Sets the designated parameter to a <code>Reader</code> object.  The reader must contain  the number of characters
     * specified by length otherwise a <code>SQLException</code> will be generated when the
     * <code>PreparedStatement</code> is executed. This method differs from the <code>setCharacterStream (int, Reader,
     * int)</code> method because it informs the driver that the parameter value should be sent to the server as a
     * <code>NCLOB</code>.  When the <code>setCharacterStream</code> method is used, the driver may have to do extra
     * work to determine whether the parameter data should be sent to the server as a <code>LONGNVARCHAR</code> or a
     * <code>NCLOB</code>
     *
     * @param parameterIndex index of the first parameter is 1, the second is 2, ...
     * @param reader         An object that contains the data to set the parameter value to.
     * @param length         the number of characters in the parameter data.
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                               if the length specified is less than zero; if the driver does not support national
     *                               character sets; if the driver can detect that a data conversion error could occur;
     *                               if a database access error occurs or this method is called on a closed
     *                               <code>PreparedStatement</code>
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void setNClob(final int parameterIndex, final Reader reader, final long length) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("NClobs not supported");
    }

    /**
     * Sets the designated parameter to the given <code>java.sql.SQLXML</code> object. The driver converts this to an
     * SQL <code>XML</code> value when it sends it to the database.
     * <p/>
     *
     * @param parameterIndex index of the first parameter is 1, the second is 2, ...
     * @param xmlObject      a <code>SQLXML</code> object that maps an SQL <code>XML</code> value
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                               if a database access error occurs; this method is called on a closed
     *                               <code>PreparedStatement</code> or the <code>java.xml.transform.Result</code>,
     *                               <code>Writer</code> or <code>OutputStream</code> has not been closed for the
     *                               <code>SQLXML</code> object
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void setSQLXML(final int parameterIndex, final java.sql.SQLXML xmlObject) throws SQLException {
        throw SQLExceptionMapper.getFeatureNotSupportedException("SQlXML not supported");
    }

    /**
     * <p>Sets the value of the designated parameter with the given object. The second argument must be an object type;
     * for integral values, the <code>java.lang</code> equivalent objects should be used.
     * <p/>
     * If the second argument is an <code>InputStream</code> then the stream must contain the number of bytes specified
     * by scaleOrLength.  If the second argument is a <code>Reader</code> then the reader must contain the number of
     * characters specified by scaleOrLength. If these conditions are not true the driver will generate a
     * <code>SQLException</code> when the prepared statement is executed.
     * <p/>
     * <p>The given Java object will be converted to the given targetSqlType before being sent to the database.
     * <p/>
     * If the object has a custom mapping (is of a class implementing the interface <code>SQLData</code>), the JDBC
     * driver should call the method <code>SQLData.writeSQL</code> to write it to the SQL data stream. If, on the other
     * hand, the object is of a class implementing <code>Ref</code>, <code>Blob</code>, <code>Clob</code>,
     * <code>NClob</code>, <code>Struct</code>, <code>java.net.URL</code>, or <code>Array</code>, the driver should pass
     * it to the database as a value of the corresponding SQL type.
     * <p/>
     * <p>Note that this method may be used to pass database-specific abstract data types.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x              the object containing the input parameter value
     * @param targetSqlType  the SQL type (as defined in java.sql.Types) to be sent to the database. The scale argument
     *                       may further qualify this type.
     * @param scaleOrLength  for <code>java.sql.Types.DECIMAL</code> or <code>java.sql.Types.NUMERIC types</code>, this
     *                       is the number of digits after the decimal point. For Java Object types
     *                       <code>InputStream</code> and <code>Reader</code>, this is the length of the data in the
     *                       stream or reader.  For all other types, this value will be ignored.
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                               if a database access error occurs; this method is called on a closed
     *                               <code>PreparedStatement</code> or if the Java Object specified by x is an
     *                               InputStream or Reader object and the value of the scale parameter is less than
     *                               zero
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if <code>targetSqlType</code> is a <code>ARRAY</code>, <code>BLOB</code>,
     *                               <code>CLOB</code>, <code>DATALINK</code>, <code>JAVA_OBJECT</code>,
     *                               <code>NCHAR</code>, <code>NCLOB</code>, <code>NVARCHAR</code>,
     *                               <code>LONGNVARCHAR</code>, <code>REF</code>, <code>ROWID</code>,
     *                               <code>SQLXML</code> or  <code>STRUCT</code> data type and the JDBC driver does not
     *                               support this data type
     * @see java.sql.Types
     * @since 1.6
     */
    public void setObject(final int parameterIndex, final Object x, final int targetSqlType, final int scaleOrLength) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, targetSqlType);
            return;
        }
        switch (targetSqlType) {
            case Types.ARRAY:
            case Types.CLOB:
            case Types.DATALINK:
            case Types.NCHAR:
            case Types.NCLOB:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.REF:
            case Types.ROWID:
            case Types.SQLXML:
            case Types.STRUCT:
                throw SQLExceptionMapper.getFeatureNotSupportedException("Datatype not supported");
            case Types.INTEGER:
                if (x instanceof Number) {
                    setNumber(parameterIndex, (Number) x);
                } else {
                    setInt(parameterIndex, Integer.valueOf((String) x));
                }
        }

        throw SQLExceptionMapper.getFeatureNotSupportedException("Method not yet implemented");
    }

    private void setNumber(final int parameterIndex, final Number number) throws SQLException {
        if (number instanceof Integer) {
            setInt(parameterIndex, (Integer) number);
        } else if (number instanceof Short) {
            setShort(parameterIndex, (Short) number);
        } else {
            setLong(parameterIndex, number.longValue());
        }

    }

    /**
     * Sets the designated parameter to the given input stream, which will have the specified number of bytes. When a
     * very large ASCII value is input to a <code>LONGVARCHAR</code> parameter, it may be more practical to send it via
     * a <code>java.io.InputStream</code>. Data will be read from the stream as needed until end-of-file is reached. The
     * JDBC driver will do any necessary conversion from ASCII to the database char format.
     * <p/>
     * <P><B>Note:</B> This stream object can either be a standard Java stream object or your own subclass that
     * implements the standard interface.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x              the Java input stream that contains the ASCII parameter value
     * @param length         the number of bytes in the stream
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                               if a database access error occurs or this method is called on a closed
     *                               <code>PreparedStatement</code>
     * @since 1.6
     */
    public void setAsciiStream(final int parameterIndex, final InputStream x, final long length) throws SQLException {
        if(x == null) {
            setNull(parameterIndex, Types.VARCHAR);
            return;
        }

        try {
            setParameter(parameterIndex, new StreamParameter(x, length));
        } catch (IOException e) {
            throw SQLExceptionMapper.getSQLException("Could not read stream", e);
        }
    }

    /**
     * Sets the designated parameter to the given input stream, which will have the specified number of bytes. When a
     * very large binary value is input to a <code>LONGVARBINARY</code> parameter, it may be more practical to send it
     * via a <code>java.io.InputStream</code> object. The data will be read from the stream as needed until end-of-file
     * is reached.
     * <p/>
     * <P><B>Note:</B> This stream object can either be a standard Java stream object or your own subclass that
     * implements the standard interface.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x              the java input stream which contains the binary parameter value
     * @param length         the number of bytes in the stream
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                               if a database access error occurs or this method is called on a closed
     *                               <code>PreparedStatement</code>
     * @since 1.6
     */
    public void setBinaryStream(final int parameterIndex, final InputStream x, final long length) throws SQLException {
        if(x == null) {
            setNull(parameterIndex, Types.BLOB);
            return;
        }
        try {
            setParameter(parameterIndex, new StreamParameter(x, length));

        } catch (IOException e) {
            throw SQLExceptionMapper.getSQLException("Could not read stream", e);
        }
    }

    /**
     * Sets the designated parameter to the given <code>Reader</code> object, which is the given number of characters
     * long. When a very large UNICODE value is input to a <code>LONGVARCHAR</code> parameter, it may be more practical
     * to send it via a <code>java.io.Reader</code> object. The data will be read from the stream as needed until
     * end-of-file is reached.  The JDBC driver will do any necessary conversion from UNICODE to the database char
     * format.
     * <p/>
     * <P><B>Note:</B> This stream object can either be a standard Java stream object or your own subclass that
     * implements the standard interface.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param reader         the <code>java.io.Reader</code> object that contains the Unicode data
     * @param length         the number of characters in the stream
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                               if a database access error occurs or this method is called on a closed
     *                               <code>PreparedStatement</code>
     * @since 1.6
     */
    public void setCharacterStream(final int parameterIndex, final Reader reader, final long length) throws SQLException {
        if(reader == null) {
            setNull(parameterIndex, Types.BLOB);
            return;
        }

        try {
            setParameter(parameterIndex, new ReaderParameter(reader, length));
        } catch (IOException e) {
            throw SQLExceptionMapper.getSQLException("Could not read stream: " + e.getMessage(), e);
        }
    }

    /**
     * This function reads up the entire stream and stores it in memory since we need to know the length when sending it
     * to the server use the corresponding method with a length parameter if memory is an issue
     * <p/>
     * Sets the designated parameter to the given input stream. When a very large ASCII value is input to a
     * <code>LONGVARCHAR</code> parameter, it may be more practical to send it via a <code>java.io.InputStream</code>.
     * Data will be read from the stream as needed until end-of-file is reached.  The JDBC driver will do any necessary
     * conversion from ASCII to the database char format.
     * <p/>
     * <P><B>Note:</B> This stream object can either be a standard Java stream object or your own subclass that
     * implements the standard interface. <P><B>Note:</B> Consult your JDBC driver documentation to determine if it
     * might be more efficient to use a version of <code>setAsciiStream</code> which takes a length parameter.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x              the Java input stream that contains the ASCII parameter value
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                               if a database access error occurs or this method is called on a closed
     *                               <code>PreparedStatement</code>
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void setAsciiStream(final int parameterIndex, final InputStream x) throws SQLException {
        if(x == null) {
            setNull(parameterIndex, Types.BLOB);
            return;
        }

        try {
            setParameter(parameterIndex, new BufferedStreamParameter(x));
        } catch (IOException e) {
            throw SQLExceptionMapper.getSQLException("Could not read stream");
        }
    }

    /**
     * This function reads up the entire stream and stores it in memory since we need to know the length when sending it
     * to the server
     * <p/>
     * Sets the designated parameter to the given input stream. When a very large binary value is input to a
     * <code>LONGVARBINARY</code> parameter, it may be more practical to send it via a <code>java.io.InputStream</code>
     * object. The data will be read from the stream as needed until end-of-file is reached.
     * <p/>
     * <P><B>Note:</B> This stream object can either be a standard Java stream object or your own subclass that
     * implements the standard interface. <P><B>Note:</B> Consult your JDBC driver documentation to determine if it
     * might be more efficient to use a version of <code>setBinaryStream</code> which takes a length parameter.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x              the java input stream which contains the binary parameter value
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                               if a database access error occurs or this method is called on a closed
     *                               <code>PreparedStatement</code>
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void setBinaryStream(final int parameterIndex, final InputStream x) throws SQLException {
        if(x == null) {
            setNull(parameterIndex, Types.BLOB);
            return;
        }

        try {

            if(getProtocol().supportsPBMS()) {
                setParameter(parameterIndex,
                        new BlobStreamingParameter(x, getProtocol().getHost(), "9090", getProtocol().getDatabase()));
            } else {
                setParameter(parameterIndex, new BufferedStreamParameter(x));
            }
        } catch (IOException e) {
            throw SQLExceptionMapper.getSQLException("Could not read stream");
        }
    }

    /**
     * Sets the designated parameter to the given <code>Reader</code> object. When a very large UNICODE value is input
     * to a <code>LONGVARCHAR</code> parameter, it may be more practical to send it via a <code>java.io.Reader</code>
     * object. The data will be read from the stream as needed until end-of-file is reached.  The JDBC driver will do
     * any necessary conversion from UNICODE to the database char format.
     * <p/>
     * <P><B>Note:</B> This stream object can either be a standard Java stream object or your own subclass that
     * implements the standard interface. <P><B>Note:</B> Consult your JDBC driver documentation to determine if it
     * might be more efficient to use a version of <code>setCharacterStream</code> which takes a length parameter.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param reader         the <code>java.io.Reader</code> object that contains the Unicode data
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                               if a database access error occurs or this method is called on a closed
     *                               <code>PreparedStatement</code>
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void setCharacterStream(final int parameterIndex, final Reader reader) throws SQLException {
        if(reader == null) {
            setNull(parameterIndex, Types.BLOB);
            return;
        }


        try {
            setParameter(parameterIndex, new BufferedReaderParameter(reader));
        } catch (IOException e) {
            throw SQLExceptionMapper.getSQLException("Could not read reader", e);
        }
    }

    /**
     * Sets the designated parameter to a <code>Reader</code> object. The <code>Reader</code> reads the data till
     * end-of-file is reached. The driver does the necessary conversion from Java character format to the national
     * character set in the database.
     * <p/>
     * <P><B>Note:</B> This stream object can either be a standard Java stream object or your own subclass that
     * implements the standard interface. <P><B>Note:</B> Consult your JDBC driver documentation to determine if it
     * might be more efficient to use a version of <code>setNCharacterStream</code> which takes a length parameter.
     *
     * @param parameterIndex of the first parameter is 1, the second is 2, ...
     * @param value          the parameter value
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                               if the driver does not support national character sets;  if the driver can detect
     *                               that a data conversion error could occur; if a database access error occurs; or
     *                               this method is called on a closed <code>PreparedStatement</code>
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void setNCharacterStream(final int parameterIndex, final Reader value) throws SQLException {
        setCharacterStream(parameterIndex, value);
    }

    /**
     * Sets the designated parameter to a <code>Reader</code> object. This method differs from the
     * <code>setCharacterStream (int, Reader)</code> method because it informs the driver that the parameter value
     * should be sent to the server as a <code>CLOB</code>.  When the <code>setCharacterStream</code> method is used,
     * the driver may have to do extra work to determine whether the parameter data should be sent to the server as a
     * <code>LONGVARCHAR</code> or a <code>CLOB</code>
     * <p/>
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if it might be more efficient to use a
     * version of <code>setClob</code> which takes a length parameter.
     *
     * @param parameterIndex index of the first parameter is 1, the second is 2, ...
     * @param reader         An object that contains the data to set the parameter value to.
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                               if a database access error occurs; this method is called on a closed
     *                               <code>PreparedStatement</code>or if parameterIndex does not correspond to a
     *                               parameter marker in the SQL statement
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void setClob(final int parameterIndex, final Reader reader) throws SQLException {
        setCharacterStream(parameterIndex, reader);
    }

    /**
     * Sets the designated parameter to a <code>InputStream</code> object. This method differs from the
     * <code>setBinaryStream (int, InputStream)</code> method because it informs the driver that the parameter value
     * should be sent to the server as a <code>BLOB</code>.  When the <code>setBinaryStream</code> method is used, the
     * driver may have to do extra work to determine whether the parameter data should be sent to the server as a
     * <code>LONGVARBINARY</code> or a <code>BLOB</code>
     * <p/>
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if it might be more efficient to use a
     * version of <code>setBlob</code> which takes a length parameter.
     *
     * @param parameterIndex index of the first parameter is 1, the second is 2, ...
     * @param inputStream    An object that contains the data to set the parameter value to.
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                               if a database access error occurs; this method is called on a closed
     *                               <code>PreparedStatement</code> or if parameterIndex does not correspond to a
     *                               parameter marker in the SQL statement,
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void setBlob(final int parameterIndex, final InputStream inputStream) throws SQLException {
        if(inputStream == null) {
            setNull(parameterIndex, Types.BLOB);
            return;
        }


        try {
            if(getProtocol().supportsPBMS()) {
                setParameter(parameterIndex,
                        new BlobStreamingParameter(inputStream, getProtocol().getHost(), "9090", getProtocol().getDatabase()));
            } else {
                setParameter(parameterIndex, new BufferedStreamParameter(inputStream));
            }
        } catch (IOException e) {
            throw SQLExceptionMapper.getSQLException("Could not read stream");
        }


    }

    /**
     * Sets the designated parameter to a <code>Reader</code> object. This method differs from the
     * <code>setCharacterStream (int, Reader)</code> method because it informs the driver that the parameter value
     * should be sent to the server as a <code>NCLOB</code>.  When the <code>setCharacterStream</code> method is used,
     * the driver may have to do extra work to determine whether the parameter data should be sent to the server as a
     * <code>LONGNVARCHAR</code> or a <code>NCLOB</code> <P><B>Note:</B> Consult your JDBC driver documentation to
     * determine if it might be more efficient to use a version of <code>setNClob</code> which takes a length
     * parameter.
     *
     * @param parameterIndex index of the first parameter is 1, the second is 2, ...
     * @param reader         An object that contains the data to set the parameter value to.
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                               if the driver does not support national character sets; if the driver can detect
     *                               that a data conversion error could occur;  if a database access error occurs or
     *                               this method is called on a closed <code>PreparedStatement</code>
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void setNClob(final int parameterIndex, final Reader reader) throws SQLException {
        setClob(parameterIndex, reader);
    }


    public void setBoolean(final int column, final boolean value) throws SQLException {

        if(value) {

            setByte(column, (byte) 1);
        } else {
            setByte(column, (byte) 0);
        }
    }

    /**
     * Sets the designated parameter to the given Java <code>byte</code> value. The driver converts this to an SQL
     * <code>TINYINT</code> value when it sends it to the database.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x              the parameter value
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                               if a database access error occurs or this method is called on a closed
     *                               <code>PreparedStatement</code>
     */
    public void setByte(final int parameterIndex, final byte x) throws SQLException {
        setParameter(parameterIndex, new IntParameter(x));
    }

    /**
     * Sets the designated parameter to the given Java <code>short</code> value. The driver converts this to an SQL
     * <code>SMALLINT</code> value when it sends it to the database.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x              the parameter value
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                               if a database access error occurs or this method is called on a closed
     *                               <code>PreparedStatement</code>
     */
    public void setShort(final int parameterIndex, final short x) throws SQLException {
        setParameter(parameterIndex, new IntParameter(x));
    }

    public void setString(final int column, final String s) throws SQLException {
        if(s == null) {
            setNull(column, Types.VARCHAR);
            return;
        }

        setParameter(column, new StringParameter(s));
    }

    /**
     * Sets the designated parameter to the given Java array of bytes.  The driver converts this to an SQL
     * <code>VARBINARY</code> or <code>LONGVARBINARY</code> (depending on the argument's size relative to the driver's
     * limits on <code>VARBINARY</code> values) when it sends it to the database.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x              the parameter value
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                               if a database access error occurs or this method is called on a closed
     *                               <code>PreparedStatement</code>
     */
    public void setBytes(final int parameterIndex, final byte[] x) throws SQLException {
        if(x == null) {
            setNull(parameterIndex, Types.BLOB);
            return;
        }

        setParameter(parameterIndex, new ByteParameter(x));
    }

    /**
     * Sets the designated parameter to the given <code>java.sql.Date</code> value using the default time zone of the
     * virtual machine that is running the application. The driver converts this to an SQL <code>DATE</code> value when
     * it sends it to the database.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param date           the parameter value
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                               if a database access error occurs or this method is called on a closed
     *                               <code>PreparedStatement</code>
     */
    public void setDate(final int parameterIndex, final Date date) throws SQLException {
        if(date == null) {
            setNull(parameterIndex, Types.DATE);
            return;
        }


        setParameter(parameterIndex, new DateParameter(date.getTime()));
    }

    /**
     * Since Drizzle has no TIME datatype, time in milliseconds is stored in a packed integer
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x              the parameter value
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                               if a database access error occurs or this method is called on a closed
     *                               <code>PreparedStatement</code>
     * @see org.skysql.jdbc.internal.common.Utils#packTime(long)
     * @see org.skysql.jdbc.internal.common.Utils#unpackTime(int)
     *      <p/>
     *      Sets the designated parameter to the given <code>java.sql.Time</code> value. The driver converts this to an
     *      SQL <code>TIME</code> value when it sends it to the database.
     */
    public void setTime(final int parameterIndex, final Time x) throws SQLException {
        if(x == null) {
            setNull(parameterIndex, Types.TIME);
            return;
        }


        setParameter(parameterIndex, new TimeParameter(x.getTime()));
    }

    /**
     * Sets the designated parameter to the given <code>java.sql.Timestamp</code> value. The driver converts this to an
     * SQL <code>TIMESTAMP</code> value when it sends it to the database.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x              the parameter value
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                               if a database access error occurs or this method is called on a closed
     *                               <code>PreparedStatement</code>
     */
    public void setTimestamp(final int parameterIndex, final Timestamp x) throws SQLException {
        if(x == null) {
            setNull(parameterIndex, Types.TIMESTAMP);
            return;
        }


        setParameter(parameterIndex, new TimestampParameter(x.getTime()));
    }

    /**
     * Sets the designated parameter to the given input stream, which will have the specified number of bytes. When a
     * very large ASCII value is input to a <code>LONGVARCHAR</code> parameter, it may be more practical to send it via
     * a <code>java.io.InputStream</code>. Data will be read from the stream as needed until end-of-file is reached. The
     * JDBC driver will do any necessary conversion from ASCII to the database char format.
     * <p/>
     * <P><B>Note:</B> This stream object can either be a standard Java stream object or your own subclass that
     * implements the standard interface.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x              the Java input stream that contains the ASCII parameter value
     * @param length         the number of bytes in the stream
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                               if a database access error occurs or this method is called on a closed
     *                               <code>PreparedStatement</code>
     */
    public void setAsciiStream(final int parameterIndex, final InputStream x, final int length) throws SQLException {
        if(x == null) {
            setNull(parameterIndex, Types.BLOB);
            return;
        }


        try {
            setParameter(parameterIndex, new StreamParameter(x, length));
        } catch (IOException e) {
            throw SQLExceptionMapper.getSQLException("Could not read stream", e);
        }
    }

    /**
     * Sets the designated parameter to the given input stream, which will have the specified number of bytes.
     * <p/>
     * When a very large Unicode value is input to a <code>LONGVARCHAR</code> parameter, it may be more practical to
     * send it via a <code>java.io.InputStream</code> object. The data will be read from the stream as needed until
     * end-of-file is reached.  The JDBC driver will do any necessary conversion from Unicode to the database char
     * format.
     * <p/>
     * The byte format of the Unicode stream must be a Java UTF-8, as defined in the Java Virtual Machine
     * Specification.
     * <p/>
     * <P><B>Note:</B> This stream object can either be a standard Java stream object or your own subclass that
     * implements the standard interface.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x              a <code>java.io.InputStream</code> object that contains the Unicode parameter value
     * @param length         the number of bytes in the stream
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                               if a database access error occurs or this method is called on a closed
     *                               <code>PreparedStatement</code>
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @deprecated
     */
    public void setUnicodeStream(final int parameterIndex, final InputStream x, final int length) throws SQLException {
        if(x == null) {
            setNull(parameterIndex, Types.BLOB);
            return;
        }


        try {
            setParameter(parameterIndex, new StreamParameter(x, length));
        } catch (IOException e) {
            throw SQLExceptionMapper.getSQLException("Could not read stream", e);
        }
    }

    /**
     * Sets the designated parameter to the given input stream, which will have the specified number of bytes. When a
     * very large binary value is input to a <code>LONGVARBINARY</code> parameter, it may be more practical to send it
     * via a <code>java.io.InputStream</code> object. The data will be read from the stream as needed until end-of-file
     * is reached.
     * <p/>
     * <P><B>Note:</B> This stream object can either be a standard Java stream object or your own subclass that
     * implements the standard interface.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x              the java input stream which contains the binary parameter value
     * @param length         the number of bytes in the stream
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                               if a database access error occurs or this method is called on a closed
     *                               <code>PreparedStatement</code>
     */
    public void setBinaryStream(final int parameterIndex, final InputStream x, final int length) throws SQLException {
        if(x == null) {
            setNull(parameterIndex, Types.BLOB);
            return;
        }


        try {
            setParameter(parameterIndex, new StreamParameter(x, length));
        } catch (IOException e) {
            throw SQLExceptionMapper.getSQLException("Could not read stream", e);
        }
    }

    /**
     * Clears the current parameter values immediately. <P>In general, parameter values remain in force for repeated use
     * of a statement. Setting a parameter value automatically clears its previous value.  However, in some cases it is
     * useful to immediately release the resources used by the current parameter values; this can be done by calling the
     * method <code>clearParameters</code>.
     *
     * @throws java.sql.SQLException if a database access error occurs or this method is called on a closed
     *                               <code>PreparedStatement</code>
     */
    public void clearParameters() throws SQLException {
        dQuery.clearParameters();
    }

    /**
     * Sets the value of the designated parameter with the given object. This method is like the method
     * <code>setObject</code> above, except that it assumes a scale of zero.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x              the object containing the input parameter value
     * @param targetSqlType  the SQL type (as defined in java.sql.Types) to be sent to the database
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                               if a database access error occurs or this method is called on a closed
     *                               <code>PreparedStatement</code>
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if <code>targetSqlType</code> is a <code>ARRAY</code>, <code>BLOB</code>,
     *                               <code>CLOB</code>, <code>DATALINK</code>, <code>JAVA_OBJECT</code>,
     *                               <code>NCHAR</code>, <code>NCLOB</code>, <code>NVARCHAR</code>,
     *                               <code>LONGNVARCHAR</code>, <code>REF</code>, <code>ROWID</code>,
     *                               <code>SQLXML</code> or  <code>STRUCT</code> data type and the JDBC driver does not
     *                               support this data type
     * @see java.sql.Types
     */
    public void setObject(final int parameterIndex, final Object x, final int targetSqlType) throws SQLException {
        if (Utils.isJava5()) {
            switch(targetSqlType ) {
                case Types.ARRAY:
                case Types.CLOB:
                case Types.DATALINK:
                case Types.JAVA_OBJECT:
                case Types.REF:
                case Types.STRUCT:
                    throw SQLExceptionMapper.getFeatureNotSupportedException("Type not supported");
            }
        } else {
            switch(targetSqlType ) {
                case Types.ARRAY:
                case Types.CLOB:
                case Types.DATALINK:
                case Types.JAVA_OBJECT:
                case Types.NCHAR:
                case Types.NCLOB:
                case Types.NVARCHAR:
                case Types.LONGNVARCHAR:
                case Types.REF:
                case Types.ROWID:
                case Types.SQLXML:
                case Types.STRUCT:
                    throw SQLExceptionMapper.getFeatureNotSupportedException("Type not supported");
            }
        }
        
        if (x == null) {
            setNull(parameterIndex,Types.INTEGER);
        } else if (x instanceof String) {
            if(targetSqlType == Types.BLOB) {
                throw SQLExceptionMapper.getSQLException("Cannot convert a String to a Blob");
            }
            String s = (String)x;
            switch(targetSqlType) {
                case Types.TINYINT:
                case Types.SMALLINT:
                case Types.INTEGER:
                case Types.BIGINT:
                    try {
                        setLong(parameterIndex,Long.valueOf(s));
                    } catch (NumberFormatException e) {
                        throw SQLExceptionMapper.getSQLException("Could not convert ["+s+"] to "+targetSqlType,e);
                    }
                    break;
                case Types.DOUBLE:
                    try {
                        setDouble(parameterIndex,Double.valueOf(s));
                    } catch (NumberFormatException e) {
                        throw SQLExceptionMapper.getSQLException("Could not convert ["+s+"] to "+targetSqlType,e);
                    }
                    break;
                case Types.REAL:
                case Types.FLOAT:
                    try {
                        setFloat(parameterIndex,Float.valueOf(s));
                    } catch (NumberFormatException e) {
                        throw SQLExceptionMapper.getSQLException("Could not convert ["+s+"] to "+targetSqlType,e);
                    }
                    break;
                case Types.DECIMAL:
                case Types.NUMERIC:
                    try {
                        setBigDecimal(parameterIndex,new BigDecimal(s));
                    } catch (NumberFormatException e) {
                        throw SQLExceptionMapper.getSQLException("Could not convert ["+s+"] to "+targetSqlType,e);
                    }
                    break;
                case Types.BIT:
                    setBoolean(parameterIndex, Boolean.valueOf(s));
                    break;
                case Types.CHAR:
                case Types.VARCHAR:
                case Types.TIMESTAMP: 
                case Types.TIME:
                    setString(parameterIndex, s);
                    break;
                default:
                    throw SQLExceptionMapper.getSQLException("Could not convert ["+s+"] to "+targetSqlType);

            }
        } else if(x instanceof Number) {
            testNumbers(targetSqlType);
            Number bd = (Number) x;
            switch(targetSqlType) {
                case Types.TINYINT:
                case Types.SMALLINT:
                case Types.INTEGER:
                case Types.BIGINT:
                    setLong(parameterIndex,bd.longValue());
                    break;
                case Types.DOUBLE:
                    setDouble(parameterIndex, bd.doubleValue());
                    break;
                case Types.REAL:
                case Types.FLOAT:
                    setFloat(parameterIndex, bd.floatValue());
                    break;
                case Types.DECIMAL:
                case Types.NUMERIC:
                    if(x instanceof BigDecimal)
                        setBigDecimal(parameterIndex, (BigDecimal)x);
                    else
                        setLong(parameterIndex, bd.longValue());
                    break;
                case Types.BIT:
                    setBoolean(parameterIndex, bd.shortValue() != 0);
                    break;
                case Types.CHAR:
                case Types.VARCHAR:
                    setString(parameterIndex, bd.toString());
                    break;
                default:
                    throw SQLExceptionMapper.getSQLException("Could not convert ["+bd+"] to "+targetSqlType);

            }
        } else if (x instanceof byte[]) {
            if(targetSqlType == Types.BINARY || targetSqlType == Types.VARBINARY || targetSqlType == Types.LONGVARBINARY) {
                setBytes(parameterIndex, (byte[]) x);
            } else {
                throw SQLExceptionMapper.getSQLException("Can only convert a byte[] to BINARY, VARBINARY or LONGVARBINARY");
            }
        } else if (x instanceof Date) {
            setDate(parameterIndex, (Date) x);      // works even if targetSqlType is non date-column
        } else if (x instanceof Time) {
            setTime(parameterIndex, (Time) x);      // it is just a string anyway
        } else if (x instanceof Timestamp) {
            setTimestamp(parameterIndex, (Timestamp) x);
        } else if (x instanceof Boolean) {
            testNumbers(targetSqlType);
            setBoolean(parameterIndex, (Boolean) x);
        } else if (x instanceof Blob) {
            setBlob(parameterIndex, (Blob) x);
        } else {
            throw SQLExceptionMapper.getSQLException("Could not set parameter in setObject, could not convert: " + x.getClass()+" to "+ targetSqlType);

        }

    }

    private void testNumbers(int targetSqlType) throws SQLException {
        switch(targetSqlType ) {
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
            case Types.BLOB:
                throw SQLExceptionMapper.getSQLException("Cannot convert to "+targetSqlType);
        }
    }

    /**
     * <p>Sets the value of the designated parameter using the given object. The second parameter must be of type
     * <code>Object</code>; therefore, the <code>java.lang</code> equivalent objects should be used for built-in types.
     * <p/>
     * <p>The JDBC specification specifies a standard mapping from Java <code>Object</code> types to SQL types.  The
     * given argument will be converted to the corresponding SQL type before being sent to the database.
     * <p/>
     * <p>Note that this method may be used to pass datatabase- specific abstract data types, by using a driver-specific
     * Java type.
     * <p/>
     * If the object is of a class implementing the interface <code>SQLData</code>, the JDBC driver should call the
     * method <code>SQLData.writeSQL</code> to write it to the SQL data stream. If, on the other hand, the object is of
     * a class implementing <code>Ref</code>, <code>Blob</code>, <code>Clob</code>,  <code>NClob</code>,
     * <code>Struct</code>, <code>java.net.URL</code>, <code>RowId</code>, <code>SQLXML</code> or <code>Array</code>,
     * the driver should pass it to the database as a value of the corresponding SQL type.
     * <p/>
     * <b>Note:</b> Not all databases allow for a non-typed Null to be sent to the backend. For maximum portability, the
     * <code>setNull</code> or the <code>setObject(int parameterIndex, Object x, int sqlType)</code> method should be
     * used instead of <code>setObject(int parameterIndex, Object x)</code>.
     * <p/>
     * <b>Note:</b> This method throws an exception if there is an ambiguity, for example, if the object is of a class
     * implementing more than one of the interfaces named above.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x              the object containing the input parameter value
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                               if a database access error occurs; this method is called on a closed
     *                               <code>PreparedStatement</code> or the type of the given object is ambiguous
     */
    public void setObject(final int parameterIndex, final Object x) throws SQLException {
        if (x == null) {
            setNull(parameterIndex,Types.INTEGER);
        } else if (x instanceof String) {
            setString(parameterIndex, (String) x);
        } else if (x instanceof Integer) {
            setInt(parameterIndex, (Integer) x);
        } else if (x instanceof Long) {
            setLong(parameterIndex, (Long) x);
        } else if (x instanceof Short) {
            setShort(parameterIndex, (Short) x);
        } else if (x instanceof Double) {
            setDouble(parameterIndex, (Double) x);
        } else if (x instanceof Float) {
            setFloat(parameterIndex, (Float) x);
        } else if (x instanceof Byte) {
            setByte(parameterIndex, (Byte) x);
        } else if (x instanceof byte[]) {
            setBytes(parameterIndex, (byte[]) x);
        } else if (x instanceof Date) {
            setDate(parameterIndex, (Date) x);
        } else if (x instanceof Time) {
            setTime(parameterIndex, (Time) x);
        } else if (x instanceof Timestamp) {
            setTimestamp(parameterIndex, (Timestamp) x);
        } else if (x instanceof java.util.Date) {
            setTimestamp(parameterIndex, new Timestamp(((java.util.Date) x).getTime()));
        } else if (x instanceof Boolean) {
            setBoolean(parameterIndex, (Boolean) x);
        } else if (x instanceof Blob) {
            setBlob(parameterIndex, (Blob) x);
        } else if (x instanceof InputStream) {
            setBinaryStream(parameterIndex, (InputStream) x);
        } else if (x instanceof Reader) {
            setCharacterStream(parameterIndex, (Reader) x);
        } else if (x instanceof BigDecimal) {
            setBigDecimal(parameterIndex, (BigDecimal)x);
        }
        else if (x instanceof Blob) {
            setBlob(parameterIndex, (Blob)x);
        }
        else if (x instanceof Clob) {
            setClob(parameterIndex, (Clob)x);
        } else {
            try {
                setParameter(parameterIndex, new SerializableParameter(x));
            } catch (IOException e) {
                throw SQLExceptionMapper.getSQLException("Could not set serializable parameter in setObject: " + e.getMessage(), e);
            }
        }

    }

    public void setInt(final int column, final int i) throws SQLException {
        setParameter(column, new IntParameter(i));
    }

    /**
     * Sets the designated parameter to the given Java <code>long</code> value. The driver converts this to an SQL
     * <code>BIGINT</code> value when it sends it to the database.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x              the parameter value
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                               if a database access error occurs or this method is called on a closed
     *                               <code>PreparedStatement</code>
     */
    public void setLong(final int parameterIndex, final long x) throws SQLException {
        setParameter(parameterIndex, new LongParameter(x));
    }

    /**
     * Sets the designated parameter to the given Java <code>float</code> value. The driver converts this to an SQL
     * <code>REAL</code> value when it sends it to the database.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x              the parameter value
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                               if a database access error occurs or this method is called on a closed
     *                               <code>PreparedStatement</code>
     */
    public void setFloat(final int parameterIndex, final float x) throws SQLException {
        setParameter(parameterIndex, new DoubleParameter(x));
    }

    /**
     * Sets the designated parameter to the given Java <code>double</code> value. The driver converts this to an SQL
     * <code>DOUBLE</code> value when it sends it to the database.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x              the parameter value
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                               if a database access error occurs or this method is called on a closed
     *                               <code>PreparedStatement</code>
     */
    public void setDouble(final int parameterIndex, final double x) throws SQLException {
        setParameter(parameterIndex, new DoubleParameter(x));
    }

    /**
     * Sets the designated parameter to the given <code>java.math.BigDecimal</code> value. The driver converts this to
     * an SQL <code>NUMERIC</code> value when it sends it to the database.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x              the parameter value
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                               if a database access error occurs or this method is called on a closed
     *                               <code>PreparedStatement</code>
     */
    public void setBigDecimal(final int parameterIndex, final BigDecimal x) throws SQLException {
        if(x == null) {
            setNull(parameterIndex, Types.BIGINT);
            return;
        }

        setParameter(parameterIndex, new BigDecimalParameter(x));
    }
}