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

import java.sql.ParameterMetaData;
import java.sql.SQLException;

/**
 * Very basic info about the parameterized query, only reliable method is getParameterCount().
 */
public class MariaDbParameterMetaData implements ParameterMetaData {
    private final ColumnInformation[] parametersInformation;

    public MariaDbParameterMetaData(ColumnInformation[] parametersInformation) {
        this.parametersInformation = parametersInformation;
    }

    @Override
    public int getParameterCount() throws SQLException {
        return parametersInformation.length;
    }

    private ColumnInformation getParameterInformation(int param) throws SQLException {
        if (param >= 1 && param <= parametersInformation.length) {
            return parametersInformation[param - 1];
        }
        throw new SQLException("Parameter metadata out of range : param was " + param + " and must be 1 <= param <=" + parametersInformation.length,
                "07009");
    }

    @Override
    public int isNullable(final int param) throws SQLException {
        if (getParameterInformation(param).isNotNull()) {
            return ParameterMetaData.parameterNoNulls;
        } else {
            return ParameterMetaData.parameterNullable;
        }
    }

    @Override
    public boolean isSigned(int param) throws SQLException {
        return getParameterInformation(param).isSigned();
    }

    @Override
    public int getPrecision(int param) throws SQLException {
        long length = getParameterInformation(param).getLength();
        return (length > Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int) length;
    }

    @Override
    public int getScale(int param) throws SQLException {
        if (ColumnType.isNumeric(getParameterInformation(param).getColumnType())) {
            return getParameterInformation(param).getDecimals();
        }
        return 0;
    }

    @Override
    public int getParameterType(int param) throws SQLException {
        return getParameterInformation(param).getColumnType().getSqlType();
    }

    @Override
    public String getParameterTypeName(int param) throws SQLException {
        return getParameterInformation(param).getColumnType().getTypeName();
    }

    @Override
    public String getParameterClassName(int param) throws SQLException {
        return getParameterInformation(param).getColumnType().getClassName();
    }

    @Override
    public int getParameterMode(int param) throws SQLException {
        return parameterModeIn;
    }

    @Override
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

    public boolean isWrapperFor(final Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }
}
