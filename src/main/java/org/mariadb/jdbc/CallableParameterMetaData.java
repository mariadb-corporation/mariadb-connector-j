package org.mariadb.jdbc;


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

import org.mariadb.jdbc.internal.common.Utils;

import java.sql.*;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class CallableParameterMetaData implements ParameterMetaData {

    static Pattern PARAMETER_PATTERN =
            Pattern.compile("\\s*(IN\\s+|OUT\\s+|INOUT\\s+)?([\\w\\d]+)\\s+(UNSIGNED\\s+)?(\\w+)\\s*(\\([\\d,]+\\))?\\s*",
                    Pattern.CASE_INSENSITIVE);
    static Pattern RETURN_PATTERN =
            Pattern.compile("\\s*(UNSIGNED\\s+)?(\\w+)\\s*(\\([\\d,]+\\))?\\s*", Pattern.CASE_INSENSITIVE);
    CallParameter[] params;
    MariaDbConnection con;
    String name;
    boolean valid;
    boolean isFunction;
    boolean noAccessToMetadata;

    public CallableParameterMetaData(CallParameter[] params, MariaDbConnection con, String name, boolean isFunction) {
        this.params = params;
        this.con = con;
        this.name = name;
        this.isFunction = isFunction;
    }

    public void readMetadataFromDbIfRequired() throws SQLException {
        if (noAccessToMetadata || valid) {
            return;
        }
        try {
            readMetadata();
            valid = true;
        } catch (SQLException e) {
            noAccessToMetadata = true;
            throw e;
        }
    }

    int mapMariaDbTypeToJdbc(String str) {

        str = str.toUpperCase();
        switch (str) {
            case "BIT": return Types.BIT;
            case "TINYINT":  return Types.TINYINT;
            case "SMALLINT": return Types.SMALLINT;
            case "MEDIUMINT": return Types.INTEGER;
            case "INT": return Types.INTEGER;
            case "INTEGER": return Types.INTEGER;
            case "LONG": return Types.INTEGER;
            case "BIGINT": return Types.BIGINT;
            case "INT24": return Types.INTEGER;
            case "REAL": return Types.DOUBLE;
            case "FLOAT": return Types.FLOAT;
            case "DECIMAL": return Types.DECIMAL;
            case "NUMERIC": return Types.NUMERIC;
            case "DOUBLE": return Types.DOUBLE;
            case "CHAR": return Types.CHAR;
            case "VARCHAR": return Types.VARCHAR;
            case "DATE": return Types.DATE;
            case "TIME": return Types.TIME;
            case "YEAR": return Types.SMALLINT;
            case "TIMESTAMP": return Types.TIMESTAMP;
            case "DATETIME": return Types.TIMESTAMP;
            case "TINYBLOB": return Types.BINARY;
            case "BLOB": return Types.LONGVARBINARY;
            case "MEDIUMBLOB": return Types.LONGVARBINARY;
            case "LONGBLOB": return Types.LONGVARBINARY;
            case "TINYTEXT": return Types.VARCHAR;
            case "TEXT" : return Types.LONGVARCHAR;
            case "MEDIUMTEXT" : return Types.LONGVARCHAR;
            case "LONGTEXT": return Types.LONGVARCHAR;
            case "ENUM": return Types.VARCHAR;
            case "SET": return Types.VARCHAR;
            case "GEOMETRY": return Types.LONGVARBINARY;
            case "VARBINARY": return Types.VARBINARY;
            default:
                return Types.OTHER;
        }

    }


    /*
    Read procedure metadata from mysql.proc table(column param_list)
     */
    public void readMetadata() throws SQLException {
        if (noAccessToMetadata || valid) {
            return;
        }

        boolean noBackslashEscapes = false;

        if (con instanceof MariaDbConnection) {
            noBackslashEscapes = ((MariaDbConnection) con).noBackslashEscapes;
        }

        String dbname = "database()";
        String procedureNameNoDb = name;

        int dotIndex = name.indexOf('.');
        if (dotIndex > 0) {
            dbname = name.substring(0, dotIndex);
            dbname = dbname.replace("`", "");
            dbname = "'" + Utils.escapeString(dbname, noBackslashEscapes) + "'";
            procedureNameNoDb = name.substring(dotIndex + 1);
        }

        procedureNameNoDb = procedureNameNoDb.replace("`", "");
        procedureNameNoDb = "'" + Utils.escapeString(procedureNameNoDb, noBackslashEscapes) + "'";

        Statement st = con.createStatement();
        ResultSet rs = null;
        String paramList;
        String functionReturn;
        try {
            String query = "select param_list,returns from mysql.proc where db="
                    + dbname + " and name=" + procedureNameNoDb;
            rs = st.executeQuery(query);
            if (!rs.next()) {
                throw new SQLException("procedure or function " + name + "does not exist");
            }
            paramList = rs.getString(1);
            functionReturn = rs.getString(2);

        } finally {
            if (rs != null) {
                rs.close();
            }
            st.close();
        }

        // parse type of the return value (for functions)
        if (isFunction) {
            if (functionReturn == null || functionReturn.length() == 0) {
                throw new SQLException(name + "is not a function returning value");
            }
            Matcher matcher = RETURN_PATTERN.matcher(functionReturn);
            if (!matcher.matches()) {
                throw new SQLException("can not parse return value definition :" + functionReturn);
            }
            CallParameter callParameter = params[1];
            callParameter.isOutput = true;
            callParameter.isSigned = (matcher.group(1) == null);
            callParameter.typeName = matcher.group(2).trim();
            callParameter.sqlType = mapMariaDbTypeToJdbc(callParameter.typeName);
            String scale = matcher.group(3);
            if (scale != null) {
                scale = scale.replace("(", "").replace(")", "").replace(" ", "");
                callParameter.scale = Integer.valueOf(scale).intValue();
            }

        }

        String splitter = ",";
        StringTokenizer tokenizer = new StringTokenizer(paramList, splitter, false);
        int paramIndex = isFunction ? 2 : 1;

        while (tokenizer.hasMoreTokens()) {
            if (paramIndex >= params.length) {
                throw new SQLException("Invalid placeholder count in CallableStatement");
            }
            String paramDef = tokenizer.nextToken();
            Pattern pattern = Pattern.compile(".*\\([^)]*");
            Matcher matcher = pattern.matcher(paramDef);
            while (matcher.matches()) {
                paramDef += splitter + tokenizer.nextToken();
                matcher = pattern.matcher(paramDef);
            }

            Matcher matcher2 = PARAMETER_PATTERN.matcher(paramDef);
            if (!matcher2.matches()) {
                throw new SQLException("cannot parse parameter definition :" + paramDef);
            }
            String direction = matcher2.group(1);
            if (direction != null) {
                direction = direction.trim();
            }
            String paramName = matcher2.group(2);
            paramName = paramName.trim();
            final boolean isSigned = (matcher2.group(3) == null);
            String dataType = matcher2.group(4);
            dataType = dataType.trim();
            String scale = matcher2.group(5);
            if (scale != null) {
                scale = scale.trim();
            }

            CallParameter callParameter = params[paramIndex];
            if (direction == null || direction.equalsIgnoreCase("IN")) {
                callParameter.isInput = true;
            } else if (direction.equalsIgnoreCase("OUT")) {
                callParameter.isOutput = true;
            } else if (direction.equalsIgnoreCase("INOUT")) {
                callParameter.isInput = callParameter.isOutput = true;
            } else {
                throw new SQLException("unknown parameter direction " + direction + "for " + paramName);
            }
            callParameter.name = paramName;
            callParameter.typeName = dataType.toUpperCase();
            callParameter.sqlType = mapMariaDbTypeToJdbc(callParameter.typeName);
            callParameter.isSigned = isSigned;
            if (scale != null) {
                scale = scale.replace("(", "").replace(")", "").replace(" ", "");
                if (scale.contains(",")) {
                    scale = scale.substring(0, scale.indexOf(","));
                }
                callParameter.scale = Integer.valueOf(scale).intValue();
            }
            paramIndex++;
        }
    }

    public int getParameterCount() throws SQLException {
        return params.length - 1;
    }

    CallParameter getParam(int index) throws SQLException {
        if (index < 1 || index >= params.length) {
            throw new SQLException("invalid parameter index " + index);
        }
        readMetadataFromDbIfRequired();
        return params[index];
    }

    public int isNullable(int param) throws SQLException {
        return getParam(param).isNullable;
    }

    public boolean isSigned(int param) throws SQLException {
        return getParam(param).isSigned;
    }

    public int getPrecision(int param) throws SQLException {
        return getParam(param).precision;
    }

    public int getScale(int param) throws SQLException {
        return getParam(param).scale;
    }

    public int getParameterType(int param) throws SQLException {
        return getParam(param).sqlType;
    }

    public String getParameterTypeName(int param) throws SQLException {
        return getParam(param).typeName;
    }

    public String getParameterClassName(int param) throws SQLException {
        return getParam(param).className;
    }

    public int getParameterMode(int param) throws SQLException {
        CallParameter callParameter = getParam(param);
        if (callParameter.isInput && callParameter.isOutput) {
            return parameterModeInOut;
        }
        if (callParameter.isInput) {
            return parameterModeIn;
        }
        if (callParameter.isOutput) {
            return parameterModeOut;
        }
        return parameterModeUnknown;
    }

    public String getName(int param) throws SQLException {
        return getParam(param).name;
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}
