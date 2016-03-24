/*
MariaDB Client for Java

Copyright (c) 2012 Monty Program Ab.
Copyright (c) 2016 MariaDB Corporation AB.

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

Copyright (c) 2009-2011, Marcus Eriksson

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

package org.mariadb.jdbc.internal.queryresults.resultset.value;

import org.mariadb.jdbc.internal.util.ExceptionMapper;

import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.*;
import java.text.ParseException;

public abstract class AbstractValueObject implements ValueObject {

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getObject(Class<T> type) throws SQLException {
        if (type == null) {
            throw new SQLException("getObject(Class<T> type) : type cannot be null", "HY105");
        }

        if (type.equals(String.class)) {
            return (T) getString();
        } else if (type.equals(Long.TYPE)) {
            return (T) Long.valueOf(getLong());
        } else if (type.equals(Double.TYPE)) {
            return (T) Double.valueOf(getDouble());
        } else if (type.equals(Integer.TYPE)) {
            return (T) Integer.valueOf(getInt());
        } else if (type.equals(Short.TYPE)) {
            return (T) Short.valueOf(getShort());
        } else if (type.equals(Boolean.TYPE)) {
            return (T) Boolean.valueOf(getBoolean());
        } else if (type.equals(Byte.class)) {
            return (T) Byte.valueOf(getByte());
        } else if (type.equals(Byte[].class)) {
            return (T) getBytes();
        } else if (type.equals(Date.class)) {
            try {
                return (T) getDate(null);
            } catch (ParseException parseException) {
                throw ExceptionMapper.getSqlException("Could not parse column as date, was: \""
                        + getString()
                        + "\"", parseException);
            }
        } else if (type.equals(Time.class)) {
            try {
                return (T) getTime(null);
            } catch (ParseException parseException) {
                throw ExceptionMapper.getSqlException("Could not parse column as time, was: \""
                        + getString()
                        + "\"", parseException);
            }
        } else if (type.equals(Timestamp.class)) {
            try {
                return (T) getTimestamp(null);
            } catch (ParseException parseException) {
                throw ExceptionMapper.getSqlException("Could not parse column as timestamp, was: \""
                        + getString()
                        + "\"", parseException);
            }
        } else if (type.equals(BigDecimal.class)) {
            return (T) getBigDecimal();
        } else if (type.equals(Blob.class)) {
            return (T) getBlob();
        } else if (type.equals(Clob.class)) {
            return (T) getClob();
        } else if (type.equals(URL.class)) {
            try {
                return (T) new URL(getString());
            } catch (MalformedURLException e) {
                throw ExceptionMapper.getSqlException("Could not parse as URL");
            }
        } else if (type.equals(Array.class)) {
            throw ExceptionMapper.getFeatureNotSupportedException("Arrays are not supported");
        } else if (type.equals(SQLXML.class)) {
            throw ExceptionMapper.getFeatureNotSupportedException("SQLXMLs are not supported");
        } else if (type.equals(NClob.class)) {
            throw ExceptionMapper.getFeatureNotSupportedException("NClobs are not supported");
        } else if (type.equals(RowId.class)) {
            throw ExceptionMapper.getFeatureNotSupportedException("RowIds are not supported");
        }
        throw ExceptionMapper.getFeatureNotSupportedException("type " + type + " unknown");
    }


}
