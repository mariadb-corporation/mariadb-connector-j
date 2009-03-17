package org.drizzle.jdbc.internal.queryresults;

import org.drizzle.jdbc.internal.DrizzleType;
import java.util.Set;
import java.util.Collections;


/**
 * User: marcuse
 * Date: Mar 9, 2009
 * Time: 8:53:40 PM
 * To change this template use File | Settings | File Templates.
 */
public class DrizzleColumnInformation implements ColumnInformation {
    private final String catalog;
    private final String db;
    private final String table;
    private final String orgTable;
    private final String name;
    private final String orgName;
    private final short charsetNumber;
    private final long length;
    private final DrizzleType type;
    private final byte decimals;
    private final Set<ColumnFlags> flags;
    private int displayWidth=0;
    
    private DrizzleColumnInformation(Builder builder) {
        this.catalog=builder.catalog;
        this.db=builder.db;
        this.table=builder.table;
        this.orgTable=builder.originalTable;
        this.name = builder.name;
        this.orgName=builder.originalName;
        this.charsetNumber=builder.charsetNumber;
        this.length=builder.length;
        this.type = builder.type;
        this.decimals=builder.decimals;
        this.flags= Collections.unmodifiableSet(builder.flags);
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

    public DrizzleType getType() {
        return type;
    }

    public byte getDecimals() {
        return decimals;
    }

    public Set<ColumnFlags> getFlags() {
        return flags;
    }

    public void updateDisplaySize(int displayLength) {
        if(displayLength>displayWidth)
            this.displayWidth=displayLength;
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
        private DrizzleType type;
        private Set<ColumnFlags> flags;
        private byte decimals;

        public Builder catalog(String catalog) {
            this.catalog = catalog;
            return this;
        }

        public Builder db(String db) {
            this.db=db;
            return this;
        }

        public Builder table(String table) {
            this.table=table;
            return this;
        }

        public Builder originalTable(String orgTable) {
            this.originalTable=orgTable;
            return this;
        }

        public Builder name(String name) {
            this.name=name;
            return this;
        }

        public Builder originalName(String orgName) {
            this.originalName=orgName;
            return this;
        }

        public Builder skipMe(long bytesSkipped) {
            return this;
        }

        public Builder charsetNumber(short charsetNumber) {
            this.charsetNumber=charsetNumber;
            return this;
        }

        public Builder length(int length) {
            this.length=length;
            return this;
        }

        public Builder type(DrizzleType drizzleType) {
            this.type=drizzleType;
            return this;
        }

        public Builder flags(Set<ColumnFlags> columnFlags) {
            this.flags=columnFlags;
            return this;
        }

        public Builder decimals(byte decimals) {
            this.decimals=decimals;
            return this;
        }

        public DrizzleColumnInformation build() {
            return new DrizzleColumnInformation(this);
        }
    }
}
