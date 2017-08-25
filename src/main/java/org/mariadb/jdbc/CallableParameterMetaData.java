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


import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CallableParameterMetaData implements ParameterMetaData {

    private static final Pattern PARAMETER_PATTERN =
            Pattern.compile("\\s*(IN\\s+|OUT\\s+|INOUT\\s+)?([\\w\\d]+)\\s+(UNSIGNED\\s+)?(\\w+)\\s*(\\([\\d,]+\\))?\\s*",
                    Pattern.CASE_INSENSITIVE);
    private static final Pattern RETURN_PATTERN =
            Pattern.compile("\\s*(UNSIGNED\\s+)?(\\w+)\\s*(\\([\\d,]+\\))?\\s*(CHARSET\\s+)?(\\w+)?\\s*", Pattern.CASE_INSENSITIVE);
    private List<CallParameter> params;
    private final MariaDbConnection con;
    private String database;
    private final String name;
    private boolean valid;
    private boolean isFunction;

    /**
     * Retrieve Callable metaData.
     *
     * @param con        connection
     * @param database   database name
     * @param name       procedure/function name
     * @param isFunction is it a function
     */
    public CallableParameterMetaData(MariaDbConnection con, String database, String name, boolean isFunction) {
        this.params = null;
        this.con = con;
        if (database != null) {
            this.database = database.replace("`", "");
        } else {
            this.database = null;
        }
        this.name = name.replace("`", "");
        this.isFunction = isFunction;
    }

    /**
     * Search metaData if not already loaded.
     *
     * @throws SQLException if error append during loading metaData
     */
    public void readMetadataFromDbIfRequired() throws SQLException {
        if (valid) {
            return;
        }
        readMetadata();
        valid = true;
    }

    private int mapMariaDbTypeToJdbc(String str) {
        switch (str.toUpperCase()) {
            case "BIT":
                return Types.BIT;
            case "TINYINT":
                return Types.TINYINT;
            case "SMALLINT":
                return Types.SMALLINT;
            case "MEDIUMINT":
                return Types.INTEGER;
            case "INT":
                return Types.INTEGER;
            case "INTEGER":
                return Types.INTEGER;
            case "LONG":
                return Types.INTEGER;
            case "BIGINT":
                return Types.BIGINT;
            case "INT24":
                return Types.INTEGER;
            case "REAL":
                return Types.DOUBLE;
            case "FLOAT":
                return Types.FLOAT;
            case "DECIMAL":
                return Types.DECIMAL;
            case "NUMERIC":
                return Types.NUMERIC;
            case "DOUBLE":
                return Types.DOUBLE;
            case "CHAR":
                return Types.CHAR;
            case "VARCHAR":
                return Types.VARCHAR;
            case "DATE":
                return Types.DATE;
            case "TIME":
                return Types.TIME;
            case "YEAR":
                return Types.SMALLINT;
            case "TIMESTAMP":
                return Types.TIMESTAMP;
            case "DATETIME":
                return Types.TIMESTAMP;
            case "TINYBLOB":
                return Types.BINARY;
            case "BLOB":
                return Types.LONGVARBINARY;
            case "MEDIUMBLOB":
                return Types.LONGVARBINARY;
            case "LONGBLOB":
                return Types.LONGVARBINARY;
            case "TINYTEXT":
                return Types.VARCHAR;
            case "TEXT":
                return Types.LONGVARCHAR;
            case "MEDIUMTEXT":
                return Types.LONGVARCHAR;
            case "LONGTEXT":
                return Types.LONGVARCHAR;
            case "ENUM":
                return Types.VARCHAR;
            case "SET":
                return Types.VARCHAR;
            case "GEOMETRY":
                return Types.LONGVARBINARY;
            case "VARBINARY":
                return Types.VARBINARY;
            default:
                return Types.OTHER;
        }

    }


    private String[] queryMetaInfos(boolean isFunction) throws SQLException {
        String paramList;
        String functionReturn;
        try (PreparedStatement preparedStatement = con.prepareStatement(
                "select param_list, returns, db, type from mysql.proc where name=? and db="
                        + (database != null ? "?" : "DATABASE()"))) {

            preparedStatement.setString(1, name);
            if (database != null) preparedStatement.setString(2, database);

            try (ResultSet rs = preparedStatement.executeQuery()) {
                if (!rs.next()) {
                    throw new SQLException((isFunction ? "function" : "procedure") + " `" + name + "` does not exist");
                }
                paramList = rs.getString(1);
                functionReturn = rs.getString(2);
                database = rs.getString(3);
                this.isFunction = "FUNCTION".equals(rs.getString(4));
                return new String[]{paramList, functionReturn};
            }

        } catch (SQLSyntaxErrorException sqlSyntaxErrorException) {
            throw new SQLException("Access to metaData informations not granted for current user. Consider grant select access to mysql.proc "
                    + " or avoid using parameter by name", sqlSyntaxErrorException);
        }

    }

    private void parseFunctionReturnParam(String functionReturn) throws SQLException {
        if (functionReturn == null || functionReturn.length() == 0) {
            throw new SQLException(name + "is not a function returning value");
        }
        Matcher matcher = RETURN_PATTERN.matcher(functionReturn);
        if (!matcher.matches()) {
            throw new SQLException("can not parse return value definition :" + functionReturn);
        }
        CallParameter callParameter = params.get(0);
        callParameter.isOutput = true;
        callParameter.isSigned = (matcher.group(1) == null);
        callParameter.typeName = matcher.group(2).trim();
        callParameter.sqlType = mapMariaDbTypeToJdbc(callParameter.typeName);
        String scale = matcher.group(3);
        if (scale != null) {
            scale = scale.replace("(", "").replace(")", "").replace(" ", "");
            callParameter.scale = Integer.valueOf(scale);
        }
    }

    private void parseParamList(boolean isFunction, String paramList) throws SQLException {
        params = new ArrayList<>();
        if (isFunction) {
            //output parameter
            params.add(new CallParameter());
        }

        Matcher matcher2 = PARAMETER_PATTERN.matcher(paramList);
        while (matcher2.find()) {
            CallParameter callParameter = new CallParameter();
            String direction = matcher2.group(1);
            if (direction != null) {
                direction = direction.trim();
            }

            callParameter.name = matcher2.group(2).trim();
            callParameter.isSigned = (matcher2.group(3) == null);
            callParameter.typeName = matcher2.group(4).trim().toUpperCase();

            if (direction == null || direction.equalsIgnoreCase("IN")) {
                callParameter.isInput = true;
            } else if (direction.equalsIgnoreCase("OUT")) {
                callParameter.isOutput = true;
            } else if (direction.equalsIgnoreCase("INOUT")) {
                callParameter.isInput = callParameter.isOutput = true;
            } else {
                throw new SQLException("unknown parameter direction " + direction + "for " + callParameter.name);
            }

            callParameter.sqlType = mapMariaDbTypeToJdbc(callParameter.typeName);

            String scale = matcher2.group(5);
            if (scale != null) {
                scale = scale.trim().replace("(", "").replace(")", "").replace(" ", "");
                if (scale.contains(",")) {
                    scale = scale.substring(0, scale.indexOf(","));
                }
                callParameter.scale = Integer.valueOf(scale);
            }
            params.add(callParameter);
        }
    }

    /**
     * Read procedure metadata from mysql.proc table(column param_list).
     *
     * @throws SQLException if data doesn't correspond.
     */
    private void readMetadata() throws SQLException {
        if (valid) {
            return;
        }

        String[] metaInfos = queryMetaInfos(isFunction);
        String paramList = metaInfos[0];
        String functionReturn = metaInfos[1];

        parseParamList(isFunction, paramList);

        // parse type of the return value (for functions)
        if (isFunction) {
            parseFunctionReturnParam(functionReturn);
        }

    }

    public int getParameterCount() throws SQLException {
        return params.size();
    }

    private CallParameter getParam(int index) throws SQLException {
        if (index < 1 || index > params.size()) {
            throw new SQLException("invalid parameter index " + index);
        }
        readMetadataFromDbIfRequired();
        return params.get(index - 1);
    }

    public int isNullable(int param) throws SQLException {
        return getParam(param).canBeNull;
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

    /**
     * Get mode info.
     * <ul>
     * <li>0 : unknown</li>
     * <li>1 : IN</li>
     * <li>2 : INOUT</li>
     * <li>4 : OUT</li>
     * </ul>
     *
     * @param param parameter index
     * @return mode information
     * @throws SQLException if index is wrong
     */
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
