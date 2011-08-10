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

package org.skysql.jdbc.internal.mysql;

import org.skysql.jdbc.internal.common.ColumnInformation;
import org.skysql.jdbc.internal.common.DataType;
import org.skysql.jdbc.internal.common.queryresults.ColumnFlags;

import java.util.Collections;
import java.util.Set;


/**
 * User: marcuse Date: Mar 9, 2009 Time: 8:53:40 PM
 */
public class MySQLColumnInformation implements ColumnInformation {
    private final String catalog;
    private final String db;
    private final String table;
    private final String orgTable;
    private final String name;
    private final String orgName;
    private final short charsetNumber;
    private final long length;
    private final DataType type;
    private final byte decimals;
    private final Set<ColumnFlags> flags;
    private int displayWidth = 0;

    public MySQLColumnInformation(final Builder builder) {
        this.catalog = builder.catalog;
        this.db = builder.db;
        this.table = builder.table;
        this.orgTable = builder.originalTable;
        this.name = builder.name;
        this.orgName = builder.originalName;
        this.charsetNumber = builder.charsetNumber;
        this.length = builder.length;
        this.decimals = builder.decimals;
        this.flags = Collections.unmodifiableSet(builder.flags);

        if ((builder.type.getSqlType() == java.sql.Types.BLOB) && (this.charsetNumber != 63)) {
           this.type = new MySQLType(MySQLType.Type.CLOB);
        } else {
           this.type = builder.type;
        }
    }

    public String getCatalog() {
        return catalog;
    }

    public String getDb() {
        return db;
    }

    public String getTable() {
        return table;
    }

    public String getOriginalTable() {
        return orgTable;
    }

    public String getName() {
        return name;
    }

    public String getOriginalName() {
        return orgName;
    }

    public short getCharsetNumber() {
        return charsetNumber;
    }

    public long getLength() {
        return length;
    }

    public DataType getType() {
        return type;
    }

    public byte getDecimals() {
        return decimals;
    }

    public Set<ColumnFlags> getFlags() {
        return flags;
    }

    public void updateDisplaySize(final int displayLength) {
        if (displayLength > displayWidth) {
            this.displayWidth = displayLength;
        }
    }

    public static class Builder {
        private String catalog;
        private String db;
        private String table;
        private String originalTable;
        private String name;
        private String originalName;
        private short charsetNumber;
        private int length;
        private DataType type;
        private Set<ColumnFlags> flags;
        private byte decimals;

        public Builder catalog(final String catalog) {
            this.catalog = catalog;
            return this;
        }

        public Builder db(final String db) {
            this.db = db;
            return this;
        }

        public Builder table(final String table) {
            this.table = table;
            return this;
        }

        public Builder originalTable(final String orgTable) {
            this.originalTable = orgTable;
            return this;
        }

        public Builder name(final String name) {
            this.name = name;
            return this;
        }

        public Builder originalName(final String orgName) {
            this.originalName = orgName;
            return this;
        }

        public Builder skipMe(final long bytesSkipped) {
            return this;
        }

        public Builder charsetNumber(final short charsetNumber) {
            this.charsetNumber = charsetNumber;
            return this;
        }

        public Builder length(final int length) {
            this.length = length;
            return this;
        }

        public Builder type(final DataType dataType) {
            this.type = dataType;
            return this;
        }

        public Builder flags(final Set<ColumnFlags> columnFlags) {
            this.flags = columnFlags;
            return this;
        }

        public Builder decimals(final byte decimals) {
            this.decimals = decimals;
            return this;
        }

        public MySQLColumnInformation build() {
            return new MySQLColumnInformation(this);
        }
    }
}