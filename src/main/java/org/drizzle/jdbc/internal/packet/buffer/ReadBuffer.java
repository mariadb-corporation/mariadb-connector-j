package org.drizzle.jdbc.internal.packet.buffer;

import java.io.InputStream;
import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Jan 16, 2009
 * Time: 8:27:38 PM
 * To change this template use File | Settings | File Templates.
 */
public class ReadBuffer {
    private int length;
    private byte packetSeq;
    private byte[] buffer;
    private int bufferPointer=0;

    /**
     * Creates a new packet, reads length from the first three bytes
     * Also buffers the whole packet.
     * @param reader the reader to use
     * @throws java.io.IOException if it was not possible to read from the inputstream of if there is a problem parsing the length
     */
    public ReadBuffer(InputStream reader) throws IOException {
        byte [] lengthBuffer = new byte[4];
        int readBytes = reader.read(lengthBuffer,0,4);
        if(readBytes!=4) {
            throw new IOException("Could not read packet");
        }
        this.length = (lengthBuffer[0] & 0xff)+(lengthBuffer[1]<<8) + (lengthBuffer[2]<<16);
        this.packetSeq = lengthBuffer[3];
        buffer=new byte[this.length+1];
        readBytes = reader.read(buffer,0,this.length);
        if(readBytes != this.length)
            throw new IOException("Could not read packet");
        int i=0;
    }

    /**
     * Reads a string from the buffer, looks for a 0 to end the string
     * @param charset the charset to use, for example ASCII
     * @return the read string
     * @throws IOException if it is not possible to create the string from the buffer
     */
    public String readString(String charset) throws IOException {
        int startPos = bufferPointer;
        // find the end of the string
        while(bufferPointer < length && buffer[bufferPointer] != 0) {
            bufferPointer++;
        }
        String returnString = new String(buffer,startPos,bufferPointer-startPos,charset);
        if(bufferPointer < length)
            bufferPointer++; //read away the string-ending zero
        return returnString;
    }
   /**
     * read an integer (2 bytes) from the buffer;
     * @return an integer
     * @throws IOException if there are not 4 bytes left in the buffer
     */
    public int readInt() throws IOException {
        if(length - bufferPointer < 2)
            throw new IOException("Could not read integer");
        return buffer[bufferPointer++] + (buffer[bufferPointer++]<<8);
    }

    /**
     * read a long (4 bytes) from the buffer;
     * @return a long
     * @throws IOException if there are not 4 bytes left in the buffer
     */
    public long readLong() throws IOException {
        if(length - bufferPointer < 3)
            throw new IOException("Could not read long ("+length+") ("+bufferPointer+")");
        return buffer[bufferPointer++] + (buffer[bufferPointer++]<<8) +(buffer[bufferPointer++]<<16) +(buffer[bufferPointer++]<<24);
    }

    /**
     * reads a byte from the buffer
     * @return the byte
     * @throws IOException if bufferPointer exceeds the length of the buffer
     */
    public byte readByte() throws IOException {
        if(bufferPointer > length)
            throw new IOException("Could not read byte");
        return buffer[bufferPointer++];
    }
    public byte[] readRawBytes(int numberOfBytes) throws IOException {
        if(bufferPointer+numberOfBytes > length)
            throw new IOException("Could not read bytes");

        byte [] returnBytes = new byte[numberOfBytes];
        for(int i=0;i<numberOfBytes;i++){
            returnBytes[i] = buffer[bufferPointer++];
        }

        return returnBytes;
    }
    public void skipByte(){
        skipBytes(1);
    }

    public void skipBytes(int bytesToSkip){
        bufferPointer+=bytesToSkip;
    }
    public byte getByteAt(int pos) throws IOException {
        if(pos>=0 && pos < length)
            return buffer[pos];
        throw new IOException("Out of bounds");
    }

    /**
     * the length of the packet
     * @return the length
     */
    public int getLength() {
        return length;
    }

    /**
     * sets the length of the packet
     * @param length the length
     */
    public void setLength(int length) {
        this.length = length;
    }
    public long read24bitword() {
        return buffer[bufferPointer++] + (buffer[bufferPointer++]<<8) +(buffer[bufferPointer++]<<16);
    }
    public long getLengthEncodedBinary() throws IOException {
        byte type = buffer[bufferPointer++];

        if(type == (byte)251)
            return -1;
        if(type == (byte)252)
            return (long)readInt();
        if(type == (byte)253)
            return read24bitword();
        if(type == (byte)254) {
            readLong(); // TODO: FIX!!!!
            return readLong();
        }
        if(type <= 250)
            return (long)type;

        return 0;
    }

    public byte getPacketSeq() {
        return packetSeq;
    }

    public String getLengthEncodedString() throws IOException {
        long length = getLengthEncodedBinary();
        if(length==-1) return null;
        String returnString = new String(buffer,bufferPointer, (int)length, "ASCII");
        bufferPointer+=length;
        return returnString;
    }

    public long getCurrentPointer() {
        return bufferPointer;
    }
}
