package org.drizzle.jdbc.internal.packet;

import org.drizzle.jdbc.internal.packet.buffer.ReadUtil;
import org.drizzle.jdbc.internal.packet.buffer.Reader;
import org.drizzle.jdbc.internal.DrizzleType;

import java.io.IOException;
import java.io.InputStream;
import java.util.EnumSet;
import java.util.Collections;
import java.util.Set;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Jan 21, 2009
 * Time: 10:49:15 PM
 * To change this template use File | Settings | File Templates.
 */
public class FieldPacket extends ResultPacket{
/*
Bytes                      Name
 -----                      ----
 n (Length Coded String)    catalog
 n (Length Coded String)    db
 n (Length Coded String)    table
 n (Length Coded String)    org_table
 n (Length Coded String)    name
 n (Length Coded String)    org_name
 1                          (filler)
 2                          charsetnr
 4                          length
 1                          type
 2                          flags
 1                          decimals
 2                          (filler), always 0x00
 n (Length Coded Binary)    default

     */
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
    private int maxFieldLength=0;
    private final Set<FieldFlags> flags;


    public enum FieldFlags {
        NOT_NULL((short)1),
        PRIMARY_KEY((short)2),
        UNIQUE_KEY((short)4),
        MULTIPLE_KEY((short)8),
        BLOB((short)16),
        UNSIGNED((short)32),
        DECIMAL((short)64),
        BINARY((short)128),
        ENUM((short)256),
        AUTO_INCREMENT((short)512),
        TIMESTAMP((short)1024),
        SET((short)2048);
        
        private short flag;

        FieldFlags(short i) {
            this.flag=i;
        }

        public short flag() {
            return flag;
        }
    }

    public FieldPacket(InputStream istream) throws IOException {
        Reader reader = new Reader(istream);
        catalog=reader.getLengthEncodedString();
        db=reader.getLengthEncodedString();
        table=reader.getLengthEncodedString();
        orgTable=reader.getLengthEncodedString();
        name=reader.getLengthEncodedString();
        orgName=reader.getLengthEncodedString();
        reader.skipBytes(1);
        charsetNumber = reader.readShort();
        length=reader.readInt();
        type=DrizzleType.values()[reader.readByte()]; //bad, using ordial to fetch type
        flags= Collections.unmodifiableSet(parseFlags(reader.readShort()));
        decimals=reader.readByte();
        reader.skipBytes(2);
    }

    private Set<FieldFlags> parseFlags(short i) {
        Set<FieldFlags> retFlags = EnumSet.noneOf(FieldFlags.class);
        for(FieldFlags fieldFlag: FieldFlags.values()) {
            if((i & fieldFlag.flag())==fieldFlag.flag())
                retFlags.add(fieldFlag);
        }
        return retFlags;
    }

    public String getTypeName(){
        return type.toString();
    }
    public DrizzleType getFieldType() {
        return type;
    }
    public ResultType getResultType() {
        return null;
    }

    public byte getPacketSeq() {
        return 0;
    }

    public String getColumnName() {
        return name;
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

    public String getOrgTable() {
        return orgTable;
    }

    public String getOrgName() {
        return orgName;
    }

    public short getCharsetNumber() {
        return charsetNumber;
    }

    public long getLength() {
        return length;
    }

    public byte getType() {
        return (byte)type.ordinal();
    }

    public Set<FieldFlags> getFlags() {
        return flags;
    }

    public byte getDecimals() {
        return decimals;
    }

    public void updateDisplaySize(int length) {
        if(length>this.maxFieldLength)
            this.maxFieldLength=length;
    }
    public int getDisplaySize(){
        return this.maxFieldLength;
    }
}
