/*
MariaDB Client for Java

Copyright (c) 2012-2014 Monty Program Ab.
Copyright (c) 2015-2016 MariaDb Ab.

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

Copyright (c) 2009-2011, Marcus Eriksson, Trond Norbye, Stephane Giron

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


package org.mariadb.jdbc;

import org.mariadb.jdbc.internal.ColumnType;
import org.mariadb.jdbc.internal.queryresults.resultset.SelectResultSet;
import org.mariadb.jdbc.internal.util.ExceptionMapper;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.Map;

public abstract class CallableFunctionStatement extends MariaDbPreparedStatementClient implements CallableStatement, Cloneable {
    /**
     * Information about parameters, merely from registerOutputParameter() and setXXX() calls.
     */
    protected CallParameter[] params;
    protected CallableParameterMetaData parameterMetadata;

    /**
     * Constructor for getter/setter of callableStatement.
     *
     * @param connection          current connection
     * @param sql                 query
     * @param resultSetScrollType one of the following <code>ResultSet</code> constants: <code>ResultSet.TYPE_FORWARD_ONLY</code>,
     *                            <code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     * @throws SQLException if clientPrepareStatement creation throw an exception
     */
    public CallableFunctionStatement(MariaDbConnection connection, String sql, int resultSetScrollType) throws SQLException {
        super(connection, sql, resultSetScrollType);
    }

    /**
     * Clone data.
     *
     * @return Cloned .
     * @throws CloneNotSupportedException if any error occur.
     */
    public CallableFunctionStatement clone() throws CloneNotSupportedException {
        CallableFunctionStatement clone = (CallableFunctionStatement) super.clone();
        clone.params = params;
        clone.parameterMetadata = parameterMetadata;
        return clone;
    }

    /**
     * Data initialisation when parameterCount is defined.
     *
     * @param parametersCount number of parameters
     */
    public void initFunctionData(int parametersCount) {
        params = new CallParameter[parametersCount];
        for (int i = 0; i < parametersCount; i++) {
            params[i] = new CallParameter();
            if (i > 0) {
                params[i].isInput = true;
            }
        }
        // the query was in the form {?=call function()}, so the first parameter is always output
        params[0].isOutput = true;
    }

    protected abstract SelectResultSet getResult() throws SQLException;

    public ParameterMetaData getParameterMetaData() throws SQLException {
        parameterMetadata.readMetadataFromDbIfRequired();
        return parameterMetadata;
    }

    /**
     * Convert parameter name to parameter index in the query.
     *
     * @param parameterName name
     * @return index
     * @throws SQLException exception
     */
    protected int nameToIndex(String parameterName) throws SQLException {
        parameterMetadata.readMetadataFromDbIfRequired();
        for (int i = 1; i <= parameterMetadata.getParameterCount(); i++) {
            String name = parameterMetadata.getName(i);
            if (name != null && name.equalsIgnoreCase(parameterName)) {
                return i;
            }
        }
        throw new SQLException("there is no parameter with the name " + parameterName);
    }


    /**
     * Convert parameter name to output parameter index in the query.
     *
     * @param parameterName name
     * @return index
     * @throws SQLException exception
     */
    private int nameToOutputIndex(String parameterName) throws SQLException {
        for (int i = 0; i < parameterMetadata.getParameterCount(); i++) {
            String name = parameterMetadata.getName(i);
            if (name != null && name.equalsIgnoreCase(parameterName)) {
                return i;
            }
        }
        throw new SQLException("there is no parameter with the name " + parameterName);
    }

    /**
     * Convert parameter index to corresponding outputIndex.
     *
     * @param parameterIndex index
     * @return index
     * @throws SQLException exception
     */
    private int indexToOutputIndex(int parameterIndex) throws SQLException {
        return parameterIndex;
    }

    @Override
    public boolean wasNull() throws SQLException {
        return getResult().wasNull();
    }

    @Override
    public String getString(int parameterIndex) throws SQLException {
        return getResult().getString(indexToOutputIndex(parameterIndex));
    }

    @Override
    public String getString(String parameterName) throws SQLException {
        return getResult().getString(nameToOutputIndex(parameterName));
    }

    @Override
    public boolean getBoolean(int parameterIndex) throws SQLException {
        return getResult().getBoolean(indexToOutputIndex(parameterIndex));
    }

    @Override
    public boolean getBoolean(String parameterName) throws SQLException {
        return getResult().getBoolean(nameToOutputIndex(parameterName));
    }

    @Override
    public byte getByte(int parameterIndex) throws SQLException {
        return getResult().getByte(indexToOutputIndex(parameterIndex));
    }

    @Override
    public byte getByte(String parameterName) throws SQLException {
        return getResult().getByte(nameToOutputIndex(parameterName));
    }

    @Override
    public short getShort(int parameterIndex) throws SQLException {
        return getResult().getShort(indexToOutputIndex(parameterIndex));
    }

    @Override
    public short getShort(String parameterName) throws SQLException {
        return getResult().getShort(nameToOutputIndex(parameterName));
    }

    @Override
    public int getInt(String parameterName) throws SQLException {
        return getResult().getInt(nameToOutputIndex(parameterName));
    }

    @Override
    public int getInt(int parameterIndex) throws SQLException {
        return getResult().getInt(indexToOutputIndex(parameterIndex));
    }

    @Override
    public long getLong(String parameterName) throws SQLException {
        return getResult().getLong(nameToOutputIndex(parameterName));
    }

    @Override
    public long getLong(int parameterIndex) throws SQLException {
        return getResult().getLong(indexToOutputIndex(parameterIndex));
    }

    @Override
    public float getFloat(String parameterName) throws SQLException {
        return getResult().getFloat(nameToOutputIndex(parameterName));
    }

    @Override
    public float getFloat(int parameterIndex) throws SQLException {
        return getResult().getFloat(indexToOutputIndex(parameterIndex));
    }

    @Override
    public double getDouble(int parameterIndex) throws SQLException {
        return getResult().getDouble(indexToOutputIndex(parameterIndex));
    }

    @Override
    public double getDouble(String parameterName) throws SQLException {
        return getResult().getDouble(nameToOutputIndex(parameterName));
    }

    @Override
    @SuppressWarnings("deprecation")
    public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException {
        return getResult().getBigDecimal(indexToOutputIndex(parameterIndex));
    }

    @Override
    @SuppressWarnings("deprecation")
    public BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
        return getResult().getBigDecimal(indexToOutputIndex(parameterIndex));
    }

    @Override
    @SuppressWarnings("deprecation")
    public BigDecimal getBigDecimal(String parameterName) throws SQLException {
        return getResult().getBigDecimal(nameToOutputIndex(parameterName));
    }

    @Override
    public byte[] getBytes(String parameterName) throws SQLException {
        return getResult().getBytes(nameToOutputIndex(parameterName));
    }

    @Override
    public byte[] getBytes(int parameterIndex) throws SQLException {
        return getResult().getBytes(indexToOutputIndex(parameterIndex));
    }

    @Override
    public Date getDate(int parameterIndex) throws SQLException {
        return getResult().getDate(indexToOutputIndex(parameterIndex));
    }

    @Override
    public Date getDate(String parameterName) throws SQLException {
        return getResult().getDate(nameToOutputIndex(parameterName));
    }

    @Override
    public Date getDate(String parameterName, Calendar cal) throws SQLException {
        return getResult().getDate(nameToOutputIndex(parameterName), cal);
    }

    @Override
    public Date getDate(int parameterIndex, Calendar cal) throws SQLException {
        return getResult().getDate(parameterIndex, cal);
    }

    @Override
    public Time getTime(int parameterIndex, Calendar cal) throws SQLException {
        return getResult().getTime(indexToOutputIndex(parameterIndex), cal);
    }

    @Override
    public Time getTime(String parameterName) throws SQLException {
        return getResult().getTime(nameToOutputIndex(parameterName));
    }

    @Override
    public Time getTime(String parameterName, Calendar cal) throws SQLException {
        return getResult().getTime(nameToOutputIndex(parameterName), cal);
    }

    @Override
    public Time getTime(int parameterIndex) throws SQLException {
        return getResult().getTime(indexToOutputIndex(parameterIndex));
    }

    @Override
    public Timestamp getTimestamp(int parameterIndex) throws SQLException {
        return getResult().getTimestamp(parameterIndex);
    }

    @Override
    public Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException {
        return getResult().getTimestamp(indexToOutputIndex(parameterIndex), cal);
    }

    @Override
    public Timestamp getTimestamp(String parameterName) throws SQLException {
        return getResult().getTimestamp(nameToOutputIndex(parameterName));
    }


    @Override
    public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException {
        return getResult().getTimestamp(nameToOutputIndex(parameterName), cal);
    }

    @Override
    public Object getObject(int parameterIndex) throws SQLException {
        Class<?> classType = ColumnType.classFromJavaType(getParameter(parameterIndex).outputSqlType);
        if (classType != null) {
            return getResult().getObject(indexToOutputIndex(parameterIndex), classType);
        }
        return getResult().getObject(indexToOutputIndex(parameterIndex));

    }

    @Override
    public Object getObject(String parameterName) throws SQLException {
        int index = nameToIndex(parameterName);
        Class<?> classType = ColumnType.classFromJavaType(getParameter(index).outputSqlType);
        if (classType != null) {
            return getResult().getObject(indexToOutputIndex(index), classType);
        }
        return getResult().getObject(indexToOutputIndex(index));
    }

    @Override
    public Object getObject(int parameterIndex, Map<String, Class<?>> map) throws SQLException {
        return getResult().getObject(indexToOutputIndex(parameterIndex), map);
    }

    @Override
    public Object getObject(String parameterName, Map<String, Class<?>> map) throws SQLException {
        return getResult().getObject(nameToOutputIndex(parameterName), map);
    }

    @Override
    public <T> T getObject(int parameterIndex, Class<T> type) throws SQLException {
        return getResult().getObject(indexToOutputIndex(parameterIndex), type);
    }

    @Override
    public <T> T getObject(String parameterName, Class<T> type) throws SQLException {
        return getResult().getObject(nameToOutputIndex(parameterName), type);
    }

    @Override
    public Ref getRef(int parameterIndex) throws SQLException {
        return getResult().getRef(indexToOutputIndex(parameterIndex));
    }

    @Override
    public Ref getRef(String parameterName) throws SQLException {
        return getResult().getRef(nameToOutputIndex(parameterName));
    }

    @Override
    public Blob getBlob(int parameterIndex) throws SQLException {
        return getResult().getBlob(parameterIndex);
    }

    @Override
    public Blob getBlob(String parameterName) throws SQLException {
        return getResult().getBlob(nameToOutputIndex(parameterName));
    }

    @Override
    public Clob getClob(String parameterName) throws SQLException {
        return getResult().getClob(nameToOutputIndex(parameterName));
    }

    @Override
    public Clob getClob(int parameterIndex) throws SQLException {
        return getResult().getClob(indexToOutputIndex(parameterIndex));
    }

    @Override
    public Array getArray(String parameterName) throws SQLException {
        return getResult().getArray(nameToOutputIndex(parameterName));
    }

    @Override
    public Array getArray(int parameterIndex) throws SQLException {
        return getResult().getArray(indexToOutputIndex(parameterIndex));
    }

    @Override
    public URL getURL(int parameterIndex) throws SQLException {
        return getResult().getURL(indexToOutputIndex(parameterIndex));
    }

    @Override
    public URL getURL(String parameterName) throws SQLException {
        return getResult().getURL(nameToOutputIndex(parameterName));
    }


    @Override
    public RowId getRowId(int parameterIndex) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("RowIDs not supported");
    }

    @Override
    public RowId getRowId(String parameterName) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("RowIDs not supported");
    }

    @Override
    public NClob getNClob(int parameterIndex) throws SQLException {
        return getResult().getNClob(indexToOutputIndex(parameterIndex));
    }

    @Override
    public NClob getNClob(String parameterName) throws SQLException {
        return getResult().getNClob(nameToOutputIndex(parameterName));
    }

    @Override
    public SQLXML getSQLXML(int parameterIndex) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("SQLXML not supported");
    }

    @Override
    public SQLXML getSQLXML(String parameterName) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("SQLXML not supported");
    }

    @Override
    public String getNString(int parameterIndex) throws SQLException {
        return getResult().getNString(indexToOutputIndex(parameterIndex));
    }

    @Override
    public String getNString(String parameterName) throws SQLException {
        return getResult().getNString(nameToOutputIndex(parameterName));
    }

    @Override
    public Reader getNCharacterStream(int parameterIndex) throws SQLException {
        return getResult().getNCharacterStream(indexToOutputIndex(parameterIndex));
    }

    @Override
    public Reader getNCharacterStream(String parameterName) throws SQLException {
        return getResult().getNCharacterStream(nameToOutputIndex(parameterName));
    }

    @Override
    public Reader getCharacterStream(int parameterIndex) throws SQLException {
        return getResult().getCharacterStream(indexToOutputIndex(parameterIndex));
    }

    @Override
    public Reader getCharacterStream(String parameterName) throws SQLException {
        return getResult().getCharacterStream(nameToOutputIndex(parameterName));
    }

    /**
     * <p>Registers the designated output parameter.
     * This version of
     * the method <code>registerOutParameter</code>
     * should be used for a user-defined or <code>REF</code> output parameter.  Examples
     * of user-defined types include: <code>STRUCT</code>, <code>DISTINCT</code>,
     * <code>JAVA_OBJECT</code>, and named array types.</p>
     * <p>All OUT parameters must be registered
     * before a stored procedure is executed.</p>
     * <p>  For a user-defined parameter, the fully-qualified SQL
     * type name of the parameter should also be given, while a <code>REF</code>
     * parameter requires that the fully-qualified type name of the
     * referenced type be given.  A JDBC driver that does not need the
     * type code and type name information may ignore it.   To be portable,
     * however, applications should always provide these values for
     * user-defined and <code>REF</code> parameters.</p>
     * <p>Although it is intended for user-defined and <code>REF</code> parameters,
     * this method may be used to register a parameter of any JDBC type.
     * If the parameter does not have a user-defined or <code>REF</code> type, the
     * <i>typeName</i> parameter is ignored.</p>
     * <p><B>Note:</B> When reading the value of an out parameter, you
     * must use the getter method whose Java type corresponds to the
     * parameter's registered SQL type.</p>
     *
     * @param parameterIndex the first parameter is 1, the second is 2,...
     * @param sqlType        a value from {@link Types}
     * @param typeName       the fully-qualified name of an SQL structured type
     * @throws SQLException                    if the parameterIndex is not valid;
     *                                         if a database access error occurs or
     *                                         this method is called on a closed <code>CallableStatement</code>
     * @throws SQLFeatureNotSupportedException if <code>sqlType</code> is
     *                                         a <code>ARRAY</code>, <code>BLOB</code>, <code>CLOB</code>,
     *                                         <code>DATALINK</code>, <code>JAVA_OBJECT</code>, <code>NCHAR</code>,
     *                                         <code>NCLOB</code>, <code>NVARCHAR</code>, <code>LONGNVARCHAR</code>,
     *                                         <code>REF</code>, <code>ROWID</code>, <code>SQLXML</code>
     *                                         or  <code>STRUCT</code> data type and the JDBC driver does not support
     *                                         this data type
     * @see Types
     */
    public void registerOutParameter(int parameterIndex, int sqlType, String typeName) throws SQLException {
        CallParameter callParameter = getParameter(parameterIndex);
        callParameter.outputSqlType = sqlType;
        callParameter.typeName = typeName;
        callParameter.isOutput = true;
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType) throws SQLException {
        registerOutParameter(parameterIndex, sqlType, -1);
    }

    /**
     * <p>Registers the parameter in ordinal position
     * <code>parameterIndex</code> to be of JDBC type
     * <code>sqlType</code>. All OUT parameters must be registered
     * before a stored procedure is executed.</p>
     * <p>The JDBC type specified by <code>sqlType</code> for an OUT
     * parameter determines the Java type that must be used
     * in the <code>get</code> method to read the value of that parameter.</p>
     * <p>This version of <code>registerOutParameter</code> should be
     * used when the parameter is of JDBC type <code>NUMERIC</code>
     * or <code>DECIMAL</code>.</p>
     *
     * @param parameterIndex the first parameter is 1, the second is 2,
     *                       and so on
     * @param sqlType        the SQL type code defined by <code>java.sql.Types</code>.
     * @param scale          the desired number of digits to the right of the
     *                       decimal point.  It must be greater than or equal to zero.
     * @throws SQLException                    if the parameterIndex is not valid;
     *                                         if a database access error occurs or
     *                                         this method is called on a closed <code>CallableStatement</code>
     * @throws SQLFeatureNotSupportedException if <code>sqlType</code> is
     *                                         a <code>ARRAY</code>, <code>BLOB</code>, <code>CLOB</code>,
     *                                         <code>DATALINK</code>, <code>JAVA_OBJECT</code>, <code>NCHAR</code>,
     *                                         <code>NCLOB</code>, <code>NVARCHAR</code>, <code>LONGNVARCHAR</code>,
     *                                         <code>REF</code>, <code>ROWID</code>, <code>SQLXML</code>
     *                                         or  <code>STRUCT</code> data type and the JDBC driver does not support
     *                                         this data type
     * @see Types
     */
    @Override
    public void registerOutParameter(int parameterIndex, int sqlType, int scale) throws SQLException {
        CallParameter callParameter = getParameter(parameterIndex);
        callParameter.isOutput = true;
        callParameter.outputSqlType = sqlType;
        callParameter.scale = scale;
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType) throws SQLException {
        registerOutParameter(nameToIndex(parameterName), sqlType);
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType, int scale) throws SQLException {
        registerOutParameter(nameToIndex(parameterName), sqlType, scale);
    }


    @Override
    public void registerOutParameter(String parameterName, int sqlType, String typeName) throws SQLException {
        registerOutParameter(nameToIndex(parameterName), sqlType, typeName);
    }

    @Override
    public void registerOutParameter(int parameterIndex, SQLType sqlType) throws SQLException {
        registerOutParameter(parameterIndex, sqlType.getVendorTypeNumber());
    }

    @Override
    public void registerOutParameter(int parameterIndex, SQLType sqlType, int scale) throws SQLException {
        registerOutParameter(parameterIndex, sqlType.getVendorTypeNumber(), scale);
    }

    @Override
    public void registerOutParameter(int parameterIndex, SQLType sqlType, String typeName) throws SQLException {
        registerOutParameter(parameterIndex, sqlType.getVendorTypeNumber(), typeName);
    }

    @Override
    public void registerOutParameter(String parameterName, SQLType sqlType) throws SQLException {
        registerOutParameter(parameterName, sqlType.getVendorTypeNumber());
    }

    @Override
    public void registerOutParameter(String parameterName, SQLType sqlType, int scale) throws SQLException {
        registerOutParameter(parameterName, sqlType.getVendorTypeNumber(), scale);
    }

    @Override
    public void registerOutParameter(String parameterName, SQLType sqlType, String typeName) throws SQLException {
        registerOutParameter(parameterName, sqlType.getVendorTypeNumber(), typeName);
    }

    CallParameter getParameter(int index) throws SQLException {
        if (index > params.length || index <= 0) {
            throw new SQLException("No parameter with index " + (index));
        }
        return params[index - 1];
    }

    @Override
    public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("SQLXML not supported");
    }

    @Override
    public void setRowId(String parameterName, RowId rowid) throws SQLException {
        throw ExceptionMapper.getFeatureNotSupportedException("RowIDs not supported");
    }

    @Override
    public void setNString(String parameterName, String value) throws SQLException {
        setNString(nameToIndex(parameterName), value);
    }

    @Override
    public void setNCharacterStream(String parameterName, Reader reader, long length) throws SQLException {
        setCharacterStream(nameToIndex(parameterName), reader, length);
    }

    @Override
    public void setNCharacterStream(String parameterName, Reader reader) throws SQLException {
        setCharacterStream(nameToIndex(parameterName), reader);
    }

    @Override
    public void setNClob(String parameterName, NClob value) throws SQLException {
        setClob(nameToIndex(parameterName), value);
    }

    @Override
    public void setNClob(String parameterName, Reader reader, long length) throws SQLException {
        setClob(nameToIndex(parameterName), reader, length);
    }

    @Override
    public void setNClob(String parameterName, Reader reader) throws SQLException {
        setClob(nameToIndex(parameterName), reader);
    }

    @Override
    public void setClob(String parameterName, Reader reader, long length) throws SQLException {
        setClob(nameToIndex(parameterName), reader, length);
    }

    @Override
    public void setClob(String parameterName, Clob clob) throws SQLException {
        setClob(nameToIndex(parameterName), clob);
    }

    @Override
    public void setClob(String parameterName, Reader reader) throws SQLException {
        setClob(nameToIndex(parameterName), reader);
    }

    @Override
    public void setBlob(String parameterName, InputStream inputStream, long length) throws SQLException {
        setBlob(nameToIndex(parameterName), inputStream, length);
    }

    @Override
    public void setBlob(String parameterName, Blob blob) throws SQLException {
        setBlob(nameToIndex(parameterName), blob);
    }

    @Override
    public void setBlob(String parameterName, InputStream inputStream) throws SQLException {
        setBlob(nameToIndex(parameterName), inputStream);
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream inputStream, long length) throws SQLException {
        setAsciiStream(nameToIndex(parameterName), inputStream, length);
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream inputStream, int length) throws SQLException {
        setAsciiStream(nameToIndex(parameterName), inputStream, length);
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream inputStream) throws SQLException {
        setAsciiStream(nameToIndex(parameterName), inputStream);
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream inputStream, long length) throws SQLException {
        setBinaryStream(nameToIndex(parameterName), inputStream, length);
    }


    @Override
    public void setBinaryStream(String parameterName, InputStream inputStream) throws SQLException {
        setBinaryStream(nameToIndex(parameterName), inputStream);
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream inputStream, int length) throws SQLException {
        setBinaryStream(nameToIndex(parameterName), inputStream, length);
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader, long length) throws SQLException {
        setCharacterStream(nameToIndex(parameterName), reader, length);
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader) throws SQLException {
        setCharacterStream(nameToIndex(parameterName), reader);
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader, int length) throws SQLException {
        setCharacterStream(nameToIndex(parameterName), reader, length);
    }

    @Override
    public void setURL(String parameterName, URL url) throws SQLException {
        setURL(nameToIndex(parameterName), url);
    }

    @Override
    public void setNull(String parameterName, int sqlType) throws SQLException {
        setNull(nameToIndex(parameterName), sqlType);
    }

    @Override
    public void setNull(String parameterName, int sqlType, String typeName) throws SQLException {
        setNull(nameToIndex(parameterName), sqlType, typeName);
    }

    @Override
    public void setBoolean(String parameterName, boolean booleanValue) throws SQLException {
        setBoolean(nameToIndex(parameterName), booleanValue);

    }

    @Override
    public void setByte(String parameterName, byte byteValue) throws SQLException {
        setByte(nameToIndex(parameterName), byteValue);
    }

    @Override
    public void setShort(String parameterName, short shortValue) throws SQLException {
        setShort(nameToIndex(parameterName), shortValue);
    }

    @Override
    public void setInt(String parameterName, int intValue) throws SQLException {
        setInt(nameToIndex(parameterName), intValue);
    }

    @Override
    public void setLong(String parameterName, long longValue) throws SQLException {
        setLong(nameToIndex(parameterName), longValue);
    }

    @Override
    public void setFloat(String parameterName, float floatValue) throws SQLException {
        setFloat(nameToIndex(parameterName), floatValue);
    }

    @Override
    public void setDouble(String parameterName, double doubleValue) throws SQLException {
        setDouble(nameToIndex(parameterName), doubleValue);
    }

    @Override
    public void setBigDecimal(String parameterName, BigDecimal bigDecimal) throws SQLException {
        setBigDecimal(nameToIndex(parameterName), bigDecimal);
    }

    @Override
    public void setString(String parameterName, String stringValue) throws SQLException {
        setString(nameToIndex(parameterName), stringValue);
    }

    @Override
    public void setBytes(String parameterName, byte[] bytes) throws SQLException {
        setBytes(nameToIndex(parameterName), bytes);
    }

    @Override
    public void setDate(String parameterName, Date date) throws SQLException {
        setDate(nameToIndex(parameterName), date);
    }

    @Override
    public void setDate(String parameterName, Date date, Calendar cal) throws SQLException {
        setDate(nameToIndex(parameterName), date, cal);
    }

    @Override
    public void setTime(String parameterName, Time time) throws SQLException {
        setTime(nameToIndex(parameterName), time);
    }

    @Override
    public void setTime(String parameterName, Time time, Calendar cal) throws SQLException {
        setTime(nameToIndex(parameterName), time, cal);
    }

    @Override
    public void setTimestamp(String parameterName, Timestamp timestamp) throws SQLException {
        setTimestamp(nameToIndex(parameterName), timestamp);
    }

    @Override
    public void setTimestamp(String parameterName, Timestamp timestamp, Calendar cal) throws SQLException {
        setTimestamp(nameToIndex(parameterName), timestamp, cal);
    }


    @Override
    public void setObject(String parameterName, Object obj, int targetSqlType, int scale) throws SQLException {
        setObject(nameToIndex(parameterName), obj, targetSqlType, scale);
    }

    @Override
    public void setObject(String parameterName, Object obj, int targetSqlType) throws SQLException {
        setObject(nameToIndex(parameterName), obj, targetSqlType);
    }

    @Override
    public void setObject(String parameterName, Object obj) throws SQLException {
        setObject(nameToIndex(parameterName), obj);
    }

    @Override
    public void setObject(String parameterName, Object obj, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        setObject(nameToIndex(parameterName), obj, targetSqlType.getVendorTypeNumber(), scaleOrLength);
    }

    @Override
    public void setObject(String parameterName, Object obj, SQLType targetSqlType) throws SQLException {
        setObject(nameToIndex(parameterName), obj, targetSqlType.getVendorTypeNumber());
    }

}
