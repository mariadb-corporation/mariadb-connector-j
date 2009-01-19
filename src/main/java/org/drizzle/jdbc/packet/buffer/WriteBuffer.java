package org.drizzle.jdbc.packet.buffer;

import java.util.List;
import java.util.ArrayList;
import java.util.LinkedList;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Jan 16, 2009
 * Time: 8:10:24 PM
 * To change this template use File | Settings | File Templates.
 */
public class WriteBuffer {
    private List<Byte> buffer = new ArrayList<Byte>();
    
    public WriteBuffer writeByte(byte theByte) {
        buffer.add(theByte);
        return this;
    }
    public WriteBuffer writeBytes(byte theByte, int count) {
        for(int i=0;i<count;i++)
            this.writeByte(theByte);
        return this;
    }
    public WriteBuffer writeInt(int theInt){
        byte [] b = intToByteArray(theInt);
        buffer.add(b[0]);
        buffer.add(b[1]);
        return this;
    }
    public WriteBuffer writeLong(long theLong){
        byte [] b = longToByteArray(theLong);
        for(byte aB:b) buffer.add(aB);
        return this;
    }

    public WriteBuffer writeString(String str){
        byte [] strBytes = str.getBytes();
        for(byte aByte : strBytes) {
            buffer.add(aByte);
        }
        return this;
    }
    public byte[] toByteArray() {
        byte [] returnArray = new byte[buffer.size()];
        int i=0;
        for(Byte b : buffer) {
            returnArray[i++] = b;
        }
        return returnArray;
    }
    public byte[] toByteArrayWithLength(byte packetNumber) {
        long length = buffer.size();
        byte[] bufferBytes = new byte[buffer.size()+4];
        byte [] lengthBytes = longToByteArray(length);
        lengthBytes[3]=packetNumber;
        int i=0;
        for(byte aB:lengthBytes) bufferBytes[i++]=aB;
        for(Byte aB:buffer) bufferBytes[i++]=aB;
        return bufferBytes;
    }

    public static byte[] intToByteArray(int i) {
        byte [] returnArray = new byte[2];
        returnArray[0] = (byte)(i & 0xff);
        returnArray[1] = (byte)(i >>> 8);
        return returnArray;
    }
    public static byte [] longToByteArray(long l){
        byte[] returnArray = new byte[4];
        returnArray[0] = (byte)(l & 0xff);
        returnArray[1] = (byte)(l >>> 8);
        returnArray[2] = (byte)(l >>> 16);
        returnArray[3] = (byte)(l >>> 24);
        return returnArray;
    }
}
