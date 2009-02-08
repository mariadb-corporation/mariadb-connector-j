package org.drizzle.jdbc.internal.packet;

import org.drizzle.jdbc.internal.packet.buffer.ReadBuffer;

import java.io.IOException;

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
    private final int charsetNumber;
    private final long length;
    private final byte type;
    private final int flags;
    private final byte decimals;
    public FieldPacket(ReadBuffer readBuffer) throws IOException {
        catalog=readBuffer.getLengthEncodedString();
        db=readBuffer.getLengthEncodedString();
        table=readBuffer.getLengthEncodedString();
        orgTable=readBuffer.getLengthEncodedString();
        name=readBuffer.getLengthEncodedString();
        orgName=readBuffer.getLengthEncodedString();
        readBuffer.skipByte();
        charsetNumber = readBuffer.readInt();
        length=readBuffer.readLong();
        type=readBuffer.readByte();
        flags=readBuffer.readInt();
        decimals=readBuffer.readByte();
        readBuffer.skipBytes(2);
    }
    public ResultType getResultType() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public byte getPacketSeq() {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

/*    public String toString() {
        return "catalog="+catalog+" db="+db+" table="+table+
               " orgTable="+orgTable+" name="+name+" orgName="
                +orgName+" charsetNumber="+charsetNumber+" length="+
                length+" type="+type+" flags="+flags+" decimals="+decimals;
    }
*/
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

    public int getCharsetNumber() {
        return charsetNumber;
    }

    public long getLength() {
        return length;
    }

    public byte getType() {
        return type;
    }

    public int getFlags() {
        return flags;
    }

    public byte getDecimals() {
        return decimals;
    }
}
