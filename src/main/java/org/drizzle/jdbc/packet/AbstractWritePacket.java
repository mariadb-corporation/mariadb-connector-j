package org.drizzle.jdbc.packet;

import java.util.ArrayList;
import java.util.List;
import java.util.LinkedList;
import java.util.Arrays;

/**
 * Abstract representation of a package sent 
 * User: marcuse
 * Date: Jan 16, 2009
 * Time: 11:09:37 AM
 * To change this template use File | Settings | File Templates.
 */
public abstract class AbstractWritePacket {
    //private byte [] buffer;
    private List<Byte> buffer = new LinkedList<Byte>();

    public AbstractWritePacket writeByte(byte theByte) {
        buffer.add(theByte);
        return this;
    }
    public AbstractWritePacket writeBytes(byte theByte, int count) {
        for(int i=0;i<count;i++)
            this.writeByte(theByte);
        return this;
    }
    public AbstractWritePacket writeInt(int theInt){
        byte lowByte = (byte)(theInt&0xff);
        byte highByte = (byte)(theInt>>>8);
        buffer.add(lowByte);
        buffer.add(highByte);
        return this;
    }
    public AbstractWritePacket writeLong(long theLong){
        int lowInt = (int)(theLong & 0xffff);
        int highInt = (int)(theLong >>> 16);
        this.writeInt(lowInt);
        this.writeInt(highInt);
        return this;
    }

    public AbstractWritePacket writeString(String str){
        byte [] strBytes = str.getBytes();
        for(byte aByte : strBytes) {
            buffer.add(aByte);
        }
        return this;
    }
    public byte[] toByteArray() {
        byte [] returnArray = new byte[buffer.size()];
        int i=0;
        long length = returnArray.length;

        for(Byte b : buffer) {
            returnArray[i++] = b;
        }
        return returnArray;
    }
    public byte[] toByteArrayWithLength() {
        byte [] returnArray = new byte[buffer.size()+4];
        int i=4;
        long length = returnArray.length;
        returnArray[0] = (byte) (length&0xff);
        returnArray[1] = (byte) (length>>>8);
        returnArray[2] = (byte) (length>>>16);
        returnArray[3] = (byte) (length>>>24);
        for(Byte b : buffer) {
            returnArray[i++] = b;
        }
        return returnArray;
    }
}
