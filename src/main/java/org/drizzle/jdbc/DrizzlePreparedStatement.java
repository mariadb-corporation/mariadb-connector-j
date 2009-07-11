/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc;

import org.drizzle.jdbc.internal.SQLExceptionMapper;
import org.drizzle.jdbc.internal.common.ParameterizedBatchHandler;
import org.drizzle.jdbc.internal.common.Protocol;
import org.drizzle.jdbc.internal.common.QueryException;
import org.drizzle.jdbc.internal.common.query.IllegalParameterException;
import org.drizzle.jdbc.internal.common.query.ParameterizedQuery;
import org.drizzle.jdbc.internal.common.query.QueryFactory;
import org.drizzle.jdbc.internal.common.query.parameters.*;
import org.drizzle.jdbc.internal.common.queryresults.ModifyQueryResult;
import org.drizzle.jdbc.internal.common.queryresults.ResultSetType;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.logging.Logger;

/**
 * User: marcuse
 * Date: Jan 27, 2009
 * Time: 10:49:42 PM
 */
public class DrizzlePreparedStatement extends DrizzleStatement implements PreparedStatement {
    private final static Logger log = Logger.getLogger(DrizzlePreparedStatement.class.getName());
    private ParameterizedQuery dQuery;
    private final ParameterizedBatchHandler parameterizedBatchHandler;

    public DrizzlePreparedStatement(Protocol protocol,
                                    DrizzleConnection drizzleConnection,
                                    String query,
                                    QueryFactory queryFactory,
                                    ParameterizedBatchHandler parameterizedBatchHandler) {
        
        super(protocol, drizzleConnection, queryFactory);
        log.finest("Creating prepared statement for " + query);
        dQuery = queryFactory.createParameterizedQuery(query);
        this.parameterizedBatchHandler = parameterizedBatchHandler;
    }

    public ResultSet executeQuery() throws SQLException {
        try {
            setQueryResult(getProtocol().executeQuery(dQuery));
            return new DrizzleResultSet(getQueryResult(), this);
        } catch (QueryException e) {
            throw SQLExceptionMapper.get(e);
        }
    }

    /**
     * Executes the SQL statement in this <code>PreparedStatement</code> object,
     * which must be an SQL Data Manipulation Language (DML) statement, such as <code>INSERT</code>, <code>UPDATE</code> or
     * <code>DELETE</code>; or an SQL statement that returns nothing,
     * such as a DDL statement.
     *
     * @return either (1) the row count for SQL Data Manipulation Language (DML) statements
     *         or (2) 0 for SQL statements that return nothing
     * @throws java.sql.SQLException if a database access error occurs;
     *                               this method is called on a closed  <code>PreparedStatement</code>
     *                               or the SQL
     *                               statement returns a <code>ResultSet</code> object
     */
    public int executeUpdate() throws SQLException {
        try {
            setQueryResult(getProtocol().executeQuery(dQuery));
        } catch (QueryException e) {
            throw SQLExceptionMapper.get(e);
        }
        if (getQueryResult().getResultSetType() != ResultSetType.MODIFY) {
            throw new SQLException("The query returned a result set");
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
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter
     *                               marker in the SQL statement; if a database access error occurs or
     *                               this method is called on a closed <code>PreparedStatement</code>
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if <code>sqlType</code> is
     *                               a <code>ARRAY</code>, <code>BLOB</code>, <code>CLOB</code>,
     *                               <code>DATALINK</code>, <code>JAVA_OBJECT</code>, <code>NCHAR</code>,
     *                               <code>NCLOB</code>, <code>NVARCHAR</code>, <code>LONGNVARCHAR</code>,
     *                               <code>REF</code>, <code>ROWID</code>, <code>SQLXML</code>
     *                               or  <code>STRUCT</code> data type and the JDBC driver does not support
     *                               this data type
     */
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        setParameter(parameterIndex, new NullParameter());
    }

    public boolean execute() throws SQLException {
        try {
            setQueryResult(getProtocol().executeQuery(dQuery));
        } catch (QueryException e) {
            throw SQLExceptionMapper.get(e);
        }
        if (getQueryResult().getResultSetType() == ResultSetType.SELECT) {
            super.setResultSet(new DrizzleResultSet(getQueryResult(), this));
            return true;
        } else {
            setUpdateCount(((ModifyQueryResult) getQueryResult()).getUpdateCount());
            return false;
        }
    }

    /**
     * Adds a set of parameters to this <code>PreparedStatement</code>
     * object's batch of commands.
     * <p/>
     * <p/>
     *
     * @throws java.sql.SQLException if a database access error occurs or
     *                               this method is called on a closed <code>PreparedStatement</code>
     * @see java.sql.Statement#addBatch
     * @since 1.2
     */
    public void addBatch() throws SQLException {
        parameterizedBatchHandler.addToBatch(dQuery);
        dQuery = getQueryFactory().createParameterizedQuery(dQuery);
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
     * Sets the designated parameter to the given <code>Reader</code>
     * object, which is the given number of characters long.
     * When a very large UNICODE value is input to a <code>LONGVARCHAR</code>
     * parameter, it may be more practical to send it via a
     * <code>java.io.Reader</code> object. The data will be read from the stream
     * as needed until end-of-file is reached.  The JDBC driver will
     * do any necessary conversion from UNICODE to the database char format.
     * <p/>
     * <P><B>Note:</B> This stream object can either be a standard
     * Java stream object or your own subclass that implements the
     * standard interface.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param reader         the <code>java.io.Reader</code> object that contains the
     *                       Unicode data
     * @param length         the number of characters in the stream
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter
     *                               marker in the SQL statement; if a database access error occurs or
     *                               this method is called on a closed <code>PreparedStatement</code>
     * @since 1.2
     */
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        try {
            setParameter(parameterIndex, new ReaderParameter(reader, length));
        } catch (IOException e) {
            throw new SQLException("Could not read stream: " + e.getMessage(), e);

        }
    }

    /**
     * Sets the designated parameter to the given
     * <code>REF(&lt;structured-type&gt;)</code> value.
     * The driver converts this to an SQL <code>REF</code> value when it
     * sends it to the database.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x              an SQL <code>REF</code> value
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter
     *                               marker in the SQL statement; if a database access error occurs or
     *                               this method is called on a closed <code>PreparedStatement</code>
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        log.info("REFs not supported");
        throw new SQLFeatureNotSupportedException("REF not supported");
    }

    /**
     * Sets the designated parameter to the given <code>java.sql.Blob</code> object.
     * The driver converts this to an SQL <code>BLOB</code> value when it
     * sends it to the database.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x              a <code>Blob</code> object that maps an SQL <code>BLOB</code> value
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter
     *                               marker in the SQL statement; if a database access error occurs or
     *                               this method is called on a closed <code>PreparedStatement</code>
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        try {
            setParameter(parameterIndex, new StreamParameter(x.getBinaryStream(), x.length()));
        } catch (IOException e) {
            throw new SQLException("Could not read stream", e);
        }
    }

    /**
     * Sets the designated parameter to the given <code>java.sql.Clob</code> object.
     * The driver converts this to an SQL <code>CLOB</code> value when it
     * sends it to the database.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x              a <code>Clob</code> object that maps an SQL <code>CLOB</code> value
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter
     *                               marker in the SQL statement; if a database access error occurs or
     *                               this method is called on a closed <code>PreparedStatement</code>
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Clobs not supported");
    }

    /**
     * Sets the designated parameter to the given <code>java.sql.Array</code> object.
     * The driver converts this to an SQL <code>ARRAY</code> value when it
     * sends it to the database.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x              an <code>Array</code> object that maps an SQL <code>ARRAY</code> value
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter
     *                               marker in the SQL statement; if a database access error occurs or
     *                               this method is called on a closed <code>PreparedStatement</code>
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.2
     */
    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Arrays not supported");
    }

    /**
     * Retrieves a <code>ResultSetMetaData</code> object that contains
     * information about the columns of the <code>ResultSet</code> object
     * that will be returned when this <code>PreparedStatement</code> object
     * is executed.
     * <p/>
     * Because a <code>PreparedStatement</code> object is precompiled, it is
     * possible to know about the <code>ResultSet</code> object that it will
     * return without having to execute it.  Consequently, it is possible
     * to invoke the method <code>getMetaData</code> on a
     * <code>PreparedStatement</code> object rather than waiting to execute
     * it and then invoking the <code>ResultSet.getMetaData</code> method
     * on the <code>ResultSet</code> object that is returned.
     * <p/>
     * <B>NOTE:</B> Using this method may be expensive for some drivers due
     * to the lack of underlying DBMS support.
     *
     * @return the description of a <code>ResultSet</code> object's columns or
     *         <code>null</code> if the driver cannot return a
     *         <code>ResultSetMetaData</code> object
     * @throws java.sql.SQLException if a database access error occurs or
     *                               this method is called on a closed <code>PreparedStatement</code>
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support
     *                               this method
     * @since 1.2
     */
    public ResultSetMetaData getMetaData() throws SQLException {
        if (super.getResultSet() != null)
            return super.getResultSet().getMetaData();
        return null;
    }

    /**
     * Sets the designated parameter to the given <code>java.sql.Date</code> value,
     * using the given <code>Calendar</code> object.  The driver uses
     * the <code>Calendar</code> object to construct an SQL <code>DATE</code> value,
     * which the driver then sends to the database.  With
     * a <code>Calendar</code> object, the driver can calculate the date
     * taking into account a custom timezone.  If no
     * <code>Calendar</code> object is specified, the driver uses the default
     * timezone, which is that of the virtual machine running the application.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param date           the parameter value
     * @param cal            the <code>Calendar</code> object the driver will use
     *                       to construct the date
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter
     *                               marker in the SQL statement; if a database access error occurs or
     *                               this method is called on a closed <code>PreparedStatement</code>
     * @since 1.2
     */
    public void setDate(int parameterIndex, Date date, Calendar cal) throws SQLException {
        setParameter(parameterIndex, new DateParameter(date.getTime(), cal));
    }

    /**
     * Sets the designated parameter to the given <code>java.sql.Time</code> value,
     * using the given <code>Calendar</code> object.  The driver uses
     * the <code>Calendar</code> object to construct an SQL <code>TIME</code> value,
     * which the driver then sends to the database.  With
     * a <code>Calendar</code> object, the driver can calculate the time
     * taking into account a custom timezone.  If no
     * <code>Calendar</code> object is specified, the driver uses the default
     * timezone, which is that of the virtual machine running the application.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param time           the parameter value
     * @param cal            the <code>Calendar</code> object the driver will use
     *                       to construct the time
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter
     *                               marker in the SQL statement; if a database access error occurs or
     *                               this method is called on a closed <code>PreparedStatement</code>
     * @since 1.2
     */
    public void setTime(int parameterIndex, Time time, Calendar cal) throws SQLException {
//        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
//        sdf.setCalendar(cal);
        setParameter(parameterIndex, new TimeParameter(time.getTime()));
    }

    /**
     * Sets the designated parameter to the given <code>java.sql.Timestamp</code> value,
     * using the given <code>Calendar</code> object.  The driver uses
     * the <code>Calendar</code> object to construct an SQL <code>TIMESTAMP</code> value,
     * which the driver then sends to the database.  With a
     * <code>Calendar</code> object, the driver can calculate the timestamp
     * taking into account a custom timezone.  If no
     * <code>Calendar</code> object is specified, the driver uses the default
     * timezone, which is that of the virtual machine running the application.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x              the parameter value
     * @param cal            the <code>Calendar</code> object the driver will use
     *                       to construct the timestamp
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter
     *                               marker in the SQL statement; if a database access error occurs or
     *                               this method is called on a closed <code>PreparedStatement</code>
     * @since 1.2
     */
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        setParameter(parameterIndex, new TimestampParameter(x.getTime(), cal));
    }

    /**
     * Sets the designated parameter to SQL <code>NULL</code>.
     * This version of the method <code>setNull</code> should
     * be used for user-defined types and REF type parameters.  Examples
     * of user-defined types include: STRUCT, DISTINCT, JAVA_OBJECT, and
     * named array types.
     * <p/>
     * <P><B>Note:</B> To be portable, applications must give the
     * SQL type code and the fully-qualified SQL type name when specifying
     * a NULL user-defined or REF parameter.  In the case of a user-defined type
     * the name is the type name of the parameter itself.  For a REF
     * parameter, the name is the type name of the referenced type.  If
     * a JDBC driver does not need the type code or type name information,
     * it may ignore it.
     * <p/>
     * Although it is intended for user-defined and Ref parameters,
     * this method may be used to set a null parameter of any JDBC type.
     * If the parameter does not have a user-defined or REF type, the given
     * typeName is ignored.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param sqlType        a value from <code>java.sql.Types</code>
     * @param typeName       the fully-qualified name of an SQL user-defined type;
     *                       ignored if the parameter is not a user-defined type or REF
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter
     *                               marker in the SQL statement; if a database access error occurs or
     *                               this method is called on a closed <code>PreparedStatement</code>
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if <code>sqlType</code> is
     *                               a <code>ARRAY</code>, <code>BLOB</code>, <code>CLOB</code>,
     *                               <code>DATALINK</code>, <code>JAVA_OBJECT</code>, <code>NCHAR</code>,
     *                               <code>NCLOB</code>, <code>NVARCHAR</code>, <code>LONGNVARCHAR</code>,
     *                               <code>REF</code>, <code>ROWID</code>, <code>SQLXML</code>
     *                               or  <code>STRUCT</code> data type and the JDBC driver does not support
     *                               this data type or if the JDBC driver does not support this method
     * @since 1.2
     */
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        setParameter(parameterIndex, new NullParameter());
    }

    private void setParameter(int parameterIndex, ParameterHolder holder) throws SQLException {
        try {
            dQuery.setParameter(parameterIndex - 1, holder);
        } catch (IllegalParameterException e) {
            throw new SQLException("Could not set parameter", e);
        }
    }

    /**
     * Sets the designated parameter to the given <code>java.net.URL</code> value.
     * The driver converts this to an SQL <code>DATALINK</code> value
     * when it sends it to the database.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x              the <code>java.net.URL</code> object to be set
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter
     *                               marker in the SQL statement; if a database access error occurs or
     *                               this method is called on a closed <code>PreparedStatement</code>
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.4
     */
    public void setURL(int parameterIndex, URL x) throws SQLException {
        setParameter(parameterIndex, new StringParameter(x.toString()));
    }

    /**
     * Retrieves the number, types and properties of this
     * <code>PreparedStatement</code> object's parameters.
     *
     * @return a <code>ParameterMetaData</code> object that contains information
     *         about the number, types and properties for each
     *         parameter marker of this <code>PreparedStatement</code> object
     * @throws java.sql.SQLException if a database access error occurs or
     *                               this method is called on a closed <code>PreparedStatement</code>
     * @see java.sql.ParameterMetaData
     * @since 1.4
     */
    public ParameterMetaData getParameterMetaData() throws SQLException {
        //TODO: figure out how this works..
        return null;
    }

    /**
     * Sets the designated parameter to the given <code>java.sql.RowId</code> object. The
     * driver converts this to a SQL <code>ROWID</code> value when it sends it
     * to the database
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x              the parameter value
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter
     *                               marker in the SQL statement; if a database access error occurs or
     *                               this method is called on a closed <code>PreparedStatement</code>
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("RowIDs not supported");
    }

    /**
     * Sets the designated paramter to the given <code>String</code> object.
     * The driver converts this to a SQL <code>NCHAR</code> or
     * <code>NVARCHAR</code> or <code>LONGNVARCHAR</code> value
     * (depending on the argument's
     * size relative to the driver's limits on <code>NVARCHAR</code> values)
     * when it sends it to the database.
     *
     * @param parameterIndex of the first parameter is 1, the second is 2, ...
     * @param value          the parameter value
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter
     *                               marker in the SQL statement; if the driver does not support national
     *                               character sets;  if the driver can detect that a data conversion
     *                               error could occur; if a database access error occurs; or
     *                               this method is called on a closed <code>PreparedStatement</code>
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void setNString(int parameterIndex, String value) throws SQLException {
        throw new SQLFeatureNotSupportedException("NStrings not supported");
    }

    /**
     * Sets the designated parameter to a <code>Reader</code> object. The
     * <code>Reader</code> reads the data till end-of-file is reached. The
     * driver does the necessary conversion from Java character format to
     * the national character set in the database.
     *
     * @param parameterIndex of the first parameter is 1, the second is 2, ...
     * @param value          the parameter value
     * @param length         the number of characters in the parameter data.
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter
     *                               marker in the SQL statement; if the driver does not support national
     *                               character sets;  if the driver can detect that a data conversion
     *                               error could occur; if a database access error occurs; or
     *                               this method is called on a closed <code>PreparedStatement</code>
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("NCharstreams not supported");
    }

    /**
     * Sets the designated parameter to a <code>java.sql.NClob</code> object. The driver converts this to a
     * SQL <code>NCLOB</code> value when it sends it to the database.
     *
     * @param parameterIndex of the first parameter is 1, the second is 2, ...
     * @param value          the parameter value
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter
     *                               marker in the SQL statement; if the driver does not support national
     *                               character sets;  if the driver can detect that a data conversion
     *                               error could occur; if a database access error occurs; or
     *                               this method is called on a closed <code>PreparedStatement</code>
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw new SQLFeatureNotSupportedException("NClobs not supported");
    }

    /**
     * Sets the designated parameter to a <code>Reader</code> object.  The reader must contain  the number
     * of characters specified by length otherwise a <code>SQLException</code> will be
     * generated when the <code>PreparedStatement</code> is executed.
     * This method differs from the <code>setCharacterStream (int, Reader, int)</code> method
     * because it informs the driver that the parameter value should be sent to
     * the server as a <code>CLOB</code>.  When the <code>setCharacterStream</code> method is used, the
     * driver may have to do extra work to determine whether the parameter
     * data should be sent to the server as a <code>LONGVARCHAR</code> or a <code>CLOB</code>
     *
     * @param parameterIndex index of the first parameter is 1, the second is 2, ...
     * @param reader         An object that contains the data to set the parameter value to.
     * @param length         the number of characters in the parameter data.
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter
     *                               marker in the SQL statement; if a database access error occurs; this method is called on
     *                               a closed <code>PreparedStatement</code> or if the length specified is less than zero.
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Clobs not supported");
    }

    /**
     * Sets the designated parameter to a <code>InputStream</code> object.  The inputstream must contain  the number
     * of characters specified by length otherwise a <code>SQLException</code> will be
     * generated when the <code>PreparedStatement</code> is executed.
     * This method differs from the <code>setBinaryStream (int, InputStream, int)</code>
     * method because it informs the driver that the parameter value should be
     * sent to the server as a <code>BLOB</code>.  When the <code>setBinaryStream</code> method is used,
     * the driver may have to do extra work to determine whether the parameter
     * data should be sent to the server as a <code>LONGVARBINARY</code> or a <code>BLOB</code>
     *
     * @param parameterIndex index of the first parameter is 1,
     *                       the second is 2, ...
     * @param inputStream    An object that contains the data to set the parameter
     *                       value to.
     * @param length         the number of bytes in the parameter data.
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter
     *                               marker in the SQL statement; if a database access error occurs;
     *                               this method is called on a closed <code>PreparedStatement</code>;
     *                               if the length specified
     *                               is less than zero or if the number of bytes in the inputstream does not match
     *                               the specfied length.
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        try {
            setParameter(parameterIndex, new StreamParameter(inputStream, length));
        } catch (IOException e) {
            throw new SQLException("Could not read stream", e);
        }
    }

    /**
     * Sets the designated parameter to a <code>Reader</code> object.  The reader must contain  the number
     * of characters specified by length otherwise a <code>SQLException</code> will be
     * generated when the <code>PreparedStatement</code> is executed.
     * This method differs from the <code>setCharacterStream (int, Reader, int)</code> method
     * because it informs the driver that the parameter value should be sent to
     * the server as a <code>NCLOB</code>.  When the <code>setCharacterStream</code> method is used, the
     * driver may have to do extra work to determine whether the parameter
     * data should be sent to the server as a <code>LONGNVARCHAR</code> or a <code>NCLOB</code>
     *
     * @param parameterIndex index of the first parameter is 1, the second is 2, ...
     * @param reader         An object that contains the data to set the parameter value to.
     * @param length         the number of characters in the parameter data.
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter
     *                               marker in the SQL statement; if the length specified is less than zero;
     *                               if the driver does not support national character sets;
     *                               if the driver can detect that a data conversion
     *                               error could occur;  if a database access error occurs or
     *                               this method is called on a closed <code>PreparedStatement</code>
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("NClobs not supported");
    }

    /**
     * Sets the designated parameter to the given <code>java.sql.SQLXML</code> object.
     * The driver converts this to an
     * SQL <code>XML</code> value when it sends it to the database.
     * <p/>
     *
     * @param parameterIndex index of the first parameter is 1, the second is 2, ...
     * @param xmlObject      a <code>SQLXML</code> object that maps an SQL <code>XML</code> value
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter
     *                               marker in the SQL statement; if a database access error occurs;
     *                               this method is called on a closed <code>PreparedStatement</code>
     *                               or the <code>java.xml.transform.Result</code>,
     *                               <code>Writer</code> or <code>OutputStream</code> has not been closed for
     *                               the <code>SQLXML</code> object
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("SQlXML not supported");
    }

    /**
     * <p>Sets the value of the designated parameter with the given object. The second
     * argument must be an object type; for integral values, the
     * <code>java.lang</code> equivalent objects should be used.
     * <p/>
     * If the second argument is an <code>InputStream</code> then the stream must contain
     * the number of bytes specified by scaleOrLength.  If the second argument is a
     * <code>Reader</code> then the reader must contain the number of characters specified
     * by scaleOrLength. If these conditions are not true the driver will generate a
     * <code>SQLException</code> when the prepared statement is executed.
     * <p/>
     * <p>The given Java object will be converted to the given targetSqlType
     * before being sent to the database.
     * <p/>
     * If the object has a custom mapping (is of a class implementing the
     * interface <code>SQLData</code>),
     * the JDBC driver should call the method <code>SQLData.writeSQL</code> to
     * write it to the SQL data stream.
     * If, on the other hand, the object is of a class implementing
     * <code>Ref</code>, <code>Blob</code>, <code>Clob</code>,  <code>NClob</code>,
     * <code>Struct</code>, <code>java.net.URL</code>,
     * or <code>Array</code>, the driver should pass it to the database as a
     * value of the corresponding SQL type.
     * <p/>
     * <p>Note that this method may be used to pass database-specific
     * abstract data types.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x              the object containing the input parameter value
     * @param targetSqlType  the SQL type (as defined in java.sql.Types) to be
     *                       sent to the database. The scale argument may further qualify this type.
     * @param scaleOrLength  for <code>java.sql.Types.DECIMAL</code>
     *                       or <code>java.sql.Types.NUMERIC types</code>,
     *                       this is the number of digits after the decimal point. For
     *                       Java Object types <code>InputStream</code> and <code>Reader</code>,
     *                       this is the length
     *                       of the data in the stream or reader.  For all other types,
     *                       this value will be ignored.
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter
     *                               marker in the SQL statement; if a database access error occurs;
     *                               this method is called on a closed <code>PreparedStatement</code> or
     *                               if the Java Object specified by x is an InputStream
     *                               or Reader object and the value of the scale parameter is less
     *                               than zero
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if <code>targetSqlType</code> is
     *                               a <code>ARRAY</code>, <code>BLOB</code>, <code>CLOB</code>,
     *                               <code>DATALINK</code>, <code>JAVA_OBJECT</code>, <code>NCHAR</code>,
     *                               <code>NCLOB</code>, <code>NVARCHAR</code>, <code>LONGNVARCHAR</code>,
     *                               <code>REF</code>, <code>ROWID</code>, <code>SQLXML</code>
     *                               or  <code>STRUCT</code> data type and the JDBC driver does not support
     *                               this data type
     * @see java.sql.Types
     * @since 1.6
     */
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
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
                throw new SQLFeatureNotSupportedException("Datatype not supported");
            case Types.INTEGER:
                if (x instanceof Number) {
                    setNumber(parameterIndex, (Number) x);
                } else {
                    setInt(parameterIndex, Integer.valueOf((String) x));
                }
        }

        throw new SQLFeatureNotSupportedException("Method not yet implemented");
    }

    private void setNumber(int parameterIndex, Number number) throws SQLException {
        if (number instanceof Integer) {
            setInt(parameterIndex, (Integer) number);
        } else if (number instanceof Short) {
            setShort(parameterIndex, (Short) number);
        } else {
            setLong(parameterIndex, number.longValue());
        }

    }

    /**
     * Sets the designated parameter to the given input stream, which will have
     * the specified number of bytes.
     * When a very large ASCII value is input to a <code>LONGVARCHAR</code>
     * parameter, it may be more practical to send it via a
     * <code>java.io.InputStream</code>. Data will be read from the stream
     * as needed until end-of-file is reached.  The JDBC driver will
     * do any necessary conversion from ASCII to the database char format.
     * <p/>
     * <P><B>Note:</B> This stream object can either be a standard
     * Java stream object or your own subclass that implements the
     * standard interface.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x              the Java input stream that contains the ASCII parameter value
     * @param length         the number of bytes in the stream
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter
     *                               marker in the SQL statement; if a database access error occurs or
     *                               this method is called on a closed <code>PreparedStatement</code>
     * @since 1.6
     */
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        try {
            setParameter(parameterIndex, new StreamParameter(x, length));
        } catch (IOException e) {
            throw new SQLException("Could not read stream", e);
        }
    }

    /**
     * Sets the designated parameter to the given input stream, which will have
     * the specified number of bytes.
     * When a very large binary value is input to a <code>LONGVARBINARY</code>
     * parameter, it may be more practical to send it via a
     * <code>java.io.InputStream</code> object. The data will be read from the
     * stream as needed until end-of-file is reached.
     * <p/>
     * <P><B>Note:</B> This stream object can either be a standard
     * Java stream object or your own subclass that implements the
     * standard interface.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x              the java input stream which contains the binary parameter value
     * @param length         the number of bytes in the stream
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter
     *                               marker in the SQL statement; if a database access error occurs or
     *                               this method is called on a closed <code>PreparedStatement</code>
     * @since 1.6
     */
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        try {
            setParameter(parameterIndex, new StreamParameter(x, length));
        } catch (IOException e) {
            throw new SQLException("Could not read stream", e);
        }
    }

    /**
     * Sets the designated parameter to the given <code>Reader</code>
     * object, which is the given number of characters long.
     * When a very large UNICODE value is input to a <code>LONGVARCHAR</code>
     * parameter, it may be more practical to send it via a
     * <code>java.io.Reader</code> object. The data will be read from the stream
     * as needed until end-of-file is reached.  The JDBC driver will
     * do any necessary conversion from UNICODE to the database char format.
     * <p/>
     * <P><B>Note:</B> This stream object can either be a standard
     * Java stream object or your own subclass that implements the
     * standard interface.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param reader         the <code>java.io.Reader</code> object that contains the
     *                       Unicode data
     * @param length         the number of characters in the stream
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter
     *                               marker in the SQL statement; if a database access error occurs or
     *                               this method is called on a closed <code>PreparedStatement</code>
     * @since 1.6
     */
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        try {
            setParameter(parameterIndex, new ReaderParameter(reader, length));
        } catch (IOException e) {
            throw new SQLException("Could not read stream: " + e.getMessage(), e);
        }
    }

    /**
     * This function reads up the entire stream and stores it in memory since
     * we need to know the length when sending it to the server
     * use the corresponding method with a length parameter if memory is an issue
     * <p/>
     * Sets the designated parameter to the given input stream.
     * When a very large ASCII value is input to a <code>LONGVARCHAR</code>
     * parameter, it may be more practical to send it via a
     * <code>java.io.InputStream</code>. Data will be read from the stream
     * as needed until end-of-file is reached.  The JDBC driver will
     * do any necessary conversion from ASCII to the database char format.
     * <p/>
     * <P><B>Note:</B> This stream object can either be a standard
     * Java stream object or your own subclass that implements the
     * standard interface.
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if
     * it might be more efficient to use a version of
     * <code>setAsciiStream</code> which takes a length parameter.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x              the Java input stream that contains the ASCII parameter value
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter
     *                               marker in the SQL statement; if a database access error occurs or
     *                               this method is called on a closed <code>PreparedStatement</code>
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        try {
            setParameter(parameterIndex, new BufferedStreamParameter(x));
        } catch (IOException e) {
            throw new SQLException("Could not read stream");
        }
    }

    /**
     * This function reads up the entire stream and stores it in memory since
     * we need to know the length when sending it to the server
     * <p/>
     * Sets the designated parameter to the given input stream.
     * When a very large binary value is input to a <code>LONGVARBINARY</code>
     * parameter, it may be more practical to send it via a
     * <code>java.io.InputStream</code> object. The data will be read from the
     * stream as needed until end-of-file is reached.
     * <p/>
     * <P><B>Note:</B> This stream object can either be a standard
     * Java stream object or your own subclass that implements the
     * standard interface.
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if
     * it might be more efficient to use a version of
     * <code>setBinaryStream</code> which takes a length parameter.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x              the java input stream which contains the binary parameter value
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter
     *                               marker in the SQL statement; if a database access error occurs or
     *                               this method is called on a closed <code>PreparedStatement</code>
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        try {
            setParameter(parameterIndex, new BufferedStreamParameter(x));
        } catch (IOException e) {
            throw new SQLException("Could not read stream");
        }
    }

    /**
     * Sets the designated parameter to the given <code>Reader</code>
     * object.
     * When a very large UNICODE value is input to a <code>LONGVARCHAR</code>
     * parameter, it may be more practical to send it via a
     * <code>java.io.Reader</code> object. The data will be read from the stream
     * as needed until end-of-file is reached.  The JDBC driver will
     * do any necessary conversion from UNICODE to the database char format.
     * <p/>
     * <P><B>Note:</B> This stream object can either be a standard
     * Java stream object or your own subclass that implements the
     * standard interface.
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if
     * it might be more efficient to use a version of
     * <code>setCharacterStream</code> which takes a length parameter.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param reader         the <code>java.io.Reader</code> object that contains the
     *                       Unicode data
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter
     *                               marker in the SQL statement; if a database access error occurs or
     *                               this method is called on a closed <code>PreparedStatement</code>
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        try {
            setParameter(parameterIndex, new BufferedReaderParameter(reader));
        } catch (IOException e) {
            throw new SQLException("Could not read reader", e);
        }
    }

    /**
     * Sets the designated parameter to a <code>Reader</code> object. The
     * <code>Reader</code> reads the data till end-of-file is reached. The
     * driver does the necessary conversion from Java character format to
     * the national character set in the database.
     * <p/>
     * <P><B>Note:</B> This stream object can either be a standard
     * Java stream object or your own subclass that implements the
     * standard interface.
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if
     * it might be more efficient to use a version of
     * <code>setNCharacterStream</code> which takes a length parameter.
     *
     * @param parameterIndex of the first parameter is 1, the second is 2, ...
     * @param value          the parameter value
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter
     *                               marker in the SQL statement; if the driver does not support national
     *                               character sets;  if the driver can detect that a data conversion
     *                               error could occur; if a database access error occurs; or
     *                               this method is called on a closed <code>PreparedStatement</code>
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw new SQLFeatureNotSupportedException("NChars not supported");
    }

    /**
     * Sets the designated parameter to a <code>Reader</code> object.
     * This method differs from the <code>setCharacterStream (int, Reader)</code> method
     * because it informs the driver that the parameter value should be sent to
     * the server as a <code>CLOB</code>.  When the <code>setCharacterStream</code> method is used, the
     * driver may have to do extra work to determine whether the parameter
     * data should be sent to the server as a <code>LONGVARCHAR</code> or a <code>CLOB</code>
     * <p/>
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if
     * it might be more efficient to use a version of
     * <code>setClob</code> which takes a length parameter.
     *
     * @param parameterIndex index of the first parameter is 1, the second is 2, ...
     * @param reader         An object that contains the data to set the parameter value to.
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter
     *                               marker in the SQL statement; if a database access error occurs; this method is called on
     *                               a closed <code>PreparedStatement</code>or if parameterIndex does not correspond to a parameter
     *                               marker in the SQL statement
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLException("CLOBs not supported");
    }

    /**
     * Sets the designated parameter to a <code>InputStream</code> object.
     * This method differs from the <code>setBinaryStream (int, InputStream)</code>
     * method because it informs the driver that the parameter value should be
     * sent to the server as a <code>BLOB</code>.  When the <code>setBinaryStream</code> method is used,
     * the driver may have to do extra work to determine whether the parameter
     * data should be sent to the server as a <code>LONGVARBINARY</code> or a <code>BLOB</code>
     * <p/>
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if
     * it might be more efficient to use a version of
     * <code>setBlob</code> which takes a length parameter.
     *
     * @param parameterIndex index of the first parameter is 1,
     *                       the second is 2, ...
     * @param inputStream    An object that contains the data to set the parameter
     *                       value to.
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter
     *                               marker in the SQL statement; if a database access error occurs;
     *                               this method is called on a closed <code>PreparedStatement</code> or
     *                               if parameterIndex does not correspond
     *                               to a parameter marker in the SQL statement,
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        try {
            setParameter(parameterIndex, new BufferedStreamParameter(inputStream));
        } catch (IOException e) {
            throw new SQLException("Could not read stream");
        }


    }

    /**
     * Sets the designated parameter to a <code>Reader</code> object.
     * This method differs from the <code>setCharacterStream (int, Reader)</code> method
     * because it informs the driver that the parameter value should be sent to
     * the server as a <code>NCLOB</code>.  When the <code>setCharacterStream</code> method is used, the
     * driver may have to do extra work to determine whether the parameter
     * data should be sent to the server as a <code>LONGNVARCHAR</code> or a <code>NCLOB</code>
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if
     * it might be more efficient to use a version of
     * <code>setNClob</code> which takes a length parameter.
     *
     * @param parameterIndex index of the first parameter is 1, the second is 2, ...
     * @param reader         An object that contains the data to set the parameter value to.
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter
     *                               marker in the SQL statement;
     *                               if the driver does not support national character sets;
     *                               if the driver can detect that a data conversion
     *                               error could occur;  if a database access error occurs or
     *                               this method is called on a closed <code>PreparedStatement</code>
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support this method
     * @since 1.6
     */
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("NClobs not supported");
    }


    public void setBoolean(int column, boolean value) throws SQLException {
        setParameter(column, new IntParameter(value ? 1 : 0));
    }

    /**
     * Sets the designated parameter to the given Java <code>byte</code> value.
     * The driver converts this
     * to an SQL <code>TINYINT</code> value when it sends it to the database.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x              the parameter value
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter
     *                               marker in the SQL statement; if a database access error occurs or
     *                               this method is called on a closed <code>PreparedStatement</code>
     */
    public void setByte(int parameterIndex, byte x) throws SQLException {
        setParameter(parameterIndex, new IntParameter(x));
    }

    /**
     * Sets the designated parameter to the given Java <code>short</code> value.
     * The driver converts this
     * to an SQL <code>SMALLINT</code> value when it sends it to the database.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x              the parameter value
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter
     *                               marker in the SQL statement; if a database access error occurs or
     *                               this method is called on a closed <code>PreparedStatement</code>
     */
    public void setShort(int parameterIndex, short x) throws SQLException {
        setParameter(parameterIndex, new IntParameter(x));
    }

    public void setString(int column, String s) throws SQLException {
        setParameter(column, new StringParameter(s));
    }

    /**
     * Sets the designated parameter to the given Java array of bytes.  The driver converts
     * this to an SQL <code>VARBINARY</code> or <code>LONGVARBINARY</code>
     * (depending on the argument's size relative to the driver's limits on
     * <code>VARBINARY</code> values) when it sends it to the database.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x              the parameter value
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter
     *                               marker in the SQL statement; if a database access error occurs or
     *                               this method is called on a closed <code>PreparedStatement</code>
     */
    public void setBytes(int parameterIndex, byte x[]) throws SQLException {
        setParameter(parameterIndex, new ByteParameter(x));
    }

    /**
     * Sets the designated parameter to the given <code>java.sql.Date</code> value
     * using the default time zone of the virtual machine that is running
     * the application.
     * The driver converts this
     * to an SQL <code>DATE</code> value when it sends it to the database.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param date           the parameter value
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter
     *                               marker in the SQL statement; if a database access error occurs or
     *                               this method is called on a closed <code>PreparedStatement</code>
     */
    public void setDate(int parameterIndex, Date date) throws SQLException {
        setParameter(parameterIndex, new DateParameter(date.getTime()));
    }

    /**
     * Since Drizzle has no TIME datatype, time in milliseconds is stored in a packed integer
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x              the parameter value
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter
     *                               marker in the SQL statement; if a database access error occurs or
     *                               this method is called on a closed <code>PreparedStatement</code>
     * @see org.drizzle.jdbc.internal.common.Utils#packTime(long)
     * @see org.drizzle.jdbc.internal.common.Utils#unpackTime(int)
     *      <p/>
     *      Sets the designated parameter to the given <code>java.sql.Time</code> value.
     *      The driver converts this
     *      to an SQL <code>TIME</code> value when it sends it to the database.
     */
    public void setTime(int parameterIndex, Time x) throws SQLException {
        setParameter(parameterIndex, new TimeParameter(x.getTime()));
    }

    /**
     * Sets the designated parameter to the given <code>java.sql.Timestamp</code> value.
     * The driver
     * converts this to an SQL <code>TIMESTAMP</code> value when it sends it to the
     * database.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x              the parameter value
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter
     *                               marker in the SQL statement; if a database access error occurs or
     *                               this method is called on a closed <code>PreparedStatement</code>
     */
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        setParameter(parameterIndex, new TimestampParameter(x.getTime()));
    }

    /**
     * Sets the designated parameter to the given input stream, which will have
     * the specified number of bytes.
     * When a very large ASCII value is input to a <code>LONGVARCHAR</code>
     * parameter, it may be more practical to send it via a
     * <code>java.io.InputStream</code>. Data will be read from the stream
     * as needed until end-of-file is reached.  The JDBC driver will
     * do any necessary conversion from ASCII to the database char format.
     * <p/>
     * <P><B>Note:</B> This stream object can either be a standard
     * Java stream object or your own subclass that implements the
     * standard interface.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x              the Java input stream that contains the ASCII parameter value
     * @param length         the number of bytes in the stream
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter
     *                               marker in the SQL statement; if a database access error occurs or
     *                               this method is called on a closed <code>PreparedStatement</code>
     */
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        try {
            setParameter(parameterIndex, new StreamParameter(x, length));
        } catch (IOException e) {
            throw new SQLException("Could not read stream", e);
        }
    }

    /**
     * Sets the designated parameter to the given input stream, which
     * will have the specified number of bytes.
     * <p/>
     * When a very large Unicode value is input to a <code>LONGVARCHAR</code>
     * parameter, it may be more practical to send it via a
     * <code>java.io.InputStream</code> object. The data will be read from the
     * stream as needed until end-of-file is reached.  The JDBC driver will
     * do any necessary conversion from Unicode to the database char format.
     * <p/>
     * The byte format of the Unicode stream must be a Java UTF-8, as defined in the
     * Java Virtual Machine Specification.
     * <p/>
     * <P><B>Note:</B> This stream object can either be a standard
     * Java stream object or your own subclass that implements the
     * standard interface.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x              a <code>java.io.InputStream</code> object that contains the
     *                       Unicode parameter value
     * @param length         the number of bytes in the stream
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter
     *                               marker in the SQL statement; if a database access error occurs or
     *                               this method is called on a closed <code>PreparedStatement</code>
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if the JDBC driver does not support
     *                               this method
     * @deprecated
     */
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        try {
            setParameter(parameterIndex, new StreamParameter(x, length));
        } catch (IOException e) {
            throw new SQLException("Could not read stream", e);
        }
    }

    /**
     * Sets the designated parameter to the given input stream, which will have
     * the specified number of bytes.
     * When a very large binary value is input to a <code>LONGVARBINARY</code>
     * parameter, it may be more practical to send it via a
     * <code>java.io.InputStream</code> object. The data will be read from the
     * stream as needed until end-of-file is reached.
     * <p/>
     * <P><B>Note:</B> This stream object can either be a standard
     * Java stream object or your own subclass that implements the
     * standard interface.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x              the java input stream which contains the binary parameter value
     * @param length         the number of bytes in the stream
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter
     *                               marker in the SQL statement; if a database access error occurs or
     *                               this method is called on a closed <code>PreparedStatement</code>
     */
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        try {
            setParameter(parameterIndex, new StreamParameter(x, length));
        } catch (IOException e) {
            throw new SQLException("Could not read stream", e);
        }
    }

    /**
     * Clears the current parameter values immediately.
     * <P>In general, parameter values remain in force for repeated use of a
     * statement. Setting a parameter value automatically clears its
     * previous value.  However, in some cases it is useful to immediately
     * release the resources used by the current parameter values; this can
     * be done by calling the method <code>clearParameters</code>.
     *
     * @throws java.sql.SQLException if a database access error occurs or
     *                               this method is called on a closed <code>PreparedStatement</code>
     */
    public void clearParameters() throws SQLException {
        dQuery.clearParameters();
    }

    /**
     * Sets the value of the designated parameter with the given object.
     * This method is like the method <code>setObject</code>
     * above, except that it assumes a scale of zero.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x              the object containing the input parameter value
     * @param targetSqlType  the SQL type (as defined in java.sql.Types) to be
     *                       sent to the database
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter
     *                               marker in the SQL statement; if a database access error occurs or
     *                               this method is called on a closed <code>PreparedStatement</code>
     * @throws java.sql.SQLFeatureNotSupportedException
     *                               if <code>targetSqlType</code> is
     *                               a <code>ARRAY</code>, <code>BLOB</code>, <code>CLOB</code>,
     *                               <code>DATALINK</code>, <code>JAVA_OBJECT</code>, <code>NCHAR</code>,
     *                               <code>NCLOB</code>, <code>NVARCHAR</code>, <code>LONGNVARCHAR</code>,
     *                               <code>REF</code>, <code>ROWID</code>, <code>SQLXML</code>
     *                               or  <code>STRUCT</code> data type and the JDBC driver does not support
     *                               this data type
     * @see java.sql.Types
     */
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not yet implemented");
    }

    /**
     * <p>Sets the value of the designated parameter using the given object.
     * The second parameter must be of type <code>Object</code>; therefore, the
     * <code>java.lang</code> equivalent objects should be used for built-in types.
     * <p/>
     * <p>The JDBC specification specifies a standard mapping from
     * Java <code>Object</code> types to SQL types.  The given argument
     * will be converted to the corresponding SQL type before being
     * sent to the database.
     * <p/>
     * <p>Note that this method may be used to pass datatabase-
     * specific abstract data types, by using a driver-specific Java
     * type.
     * <p/>
     * If the object is of a class implementing the interface <code>SQLData</code>,
     * the JDBC driver should call the method <code>SQLData.writeSQL</code>
     * to write it to the SQL data stream.
     * If, on the other hand, the object is of a class implementing
     * <code>Ref</code>, <code>Blob</code>, <code>Clob</code>,  <code>NClob</code>,
     * <code>Struct</code>, <code>java.net.URL</code>, <code>RowId</code>, <code>SQLXML</code>
     * or <code>Array</code>, the driver should pass it to the database as a
     * value of the corresponding SQL type.
     * <p/>
     * <b>Note:</b> Not all databases allow for a non-typed Null to be sent to
     * the backend. For maximum portability, the <code>setNull</code> or the
     * <code>setObject(int parameterIndex, Object x, int sqlType)</code>
     * method should be used
     * instead of <code>setObject(int parameterIndex, Object x)</code>.
     * <p/>
     * <b>Note:</b> This method throws an exception if there is an ambiguity, for example, if the
     * object is of a class implementing more than one of the interfaces named above.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x              the object containing the input parameter value
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter
     *                               marker in the SQL statement; if a database access error occurs;
     *                               this method is called on a closed <code>PreparedStatement</code>
     *                               or the type of the given object is ambiguous
     */
    public void setObject(int parameterIndex, Object x) throws SQLException {
        if (x instanceof String)
            setString(parameterIndex, (String) x);
        else if (x instanceof Integer)
            setInt(parameterIndex, (Integer) x);
        else if (x instanceof Long)
            setLong(parameterIndex, (Long) x);
        else if (x instanceof Short)
            setShort(parameterIndex, (Short) x);
        else if (x instanceof Double)
            setDouble(parameterIndex, (Double) x);
        else if (x instanceof Float)
            setFloat(parameterIndex, (Float) x);
        else if (x instanceof Byte)
            setByte(parameterIndex, (Byte) x);
        else if (x instanceof byte[])
            setBytes(parameterIndex, (byte[]) x);
        else if (x instanceof Date)
            setDate(parameterIndex, (Date) x);
        else if (x instanceof Time)
            setTime(parameterIndex, (Time) x);
        else if (x instanceof Timestamp)
            setTimestamp(parameterIndex, (Timestamp) x);
        else if (x instanceof Boolean)
            setBoolean(parameterIndex, (Boolean) x);
        else if (x instanceof Blob)
            setBlob(parameterIndex, (Blob) x);
        else if (x instanceof InputStream)
            setBinaryStream(parameterIndex, (InputStream) x);
        else if (x instanceof Reader)
            setCharacterStream(parameterIndex, (Reader) x);
        else {
            try {
                setParameter(parameterIndex, new SerializableParameter(x));
            } catch (IOException e) {
                throw new SQLException("Could not set serializable parameter in setObject: " + e.getMessage(), e);
            }
        }

    }

    public void setInt(int column, int i) throws SQLException {
        setParameter(column, new IntParameter(i));
    }

    /**
     * Sets the designated parameter to the given Java <code>long</code> value.
     * The driver converts this
     * to an SQL <code>BIGINT</code> value when it sends it to the database.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x              the parameter value
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter
     *                               marker in the SQL statement; if a database access error occurs or
     *                               this method is called on a closed <code>PreparedStatement</code>
     */
    public void setLong(int parameterIndex, long x) throws SQLException {
        setParameter(parameterIndex, new LongParameter(x));
    }

    /**
     * Sets the designated parameter to the given Java <code>float</code> value.
     * The driver converts this
     * to an SQL <code>REAL</code> value when it sends it to the database.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x              the parameter value
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter
     *                               marker in the SQL statement; if a database access error occurs or
     *                               this method is called on a closed <code>PreparedStatement</code>
     */
    public void setFloat(int parameterIndex, float x) throws SQLException {
        setParameter(parameterIndex, new DoubleParameter(x));
    }

    /**
     * Sets the designated parameter to the given Java <code>double</code> value.
     * The driver converts this
     * to an SQL <code>DOUBLE</code> value when it sends it to the database.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x              the parameter value
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter
     *                               marker in the SQL statement; if a database access error occurs or
     *                               this method is called on a closed <code>PreparedStatement</code>
     */
    public void setDouble(int parameterIndex, double x) throws SQLException {
        setParameter(parameterIndex, new DoubleParameter(x));
    }

    /**
     * Sets the designated parameter to the given <code>java.math.BigDecimal</code> value.
     * The driver converts this to an SQL <code>NUMERIC</code> value when
     * it sends it to the database.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x              the parameter value
     * @throws java.sql.SQLException if parameterIndex does not correspond to a parameter
     *                               marker in the SQL statement; if a database access error occurs or
     *                               this method is called on a closed <code>PreparedStatement</code>
     */
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        setParameter(parameterIndex, new BigDecimalParameter(x));
    }
}