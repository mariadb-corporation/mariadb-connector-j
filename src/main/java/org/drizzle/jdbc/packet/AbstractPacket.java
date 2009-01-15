package org.drizzle.jdbc.packet;

import java.io.InputStream;
import java.io.IOException;

/**
 * Abstract packet, see the concrete implementations for actual packets
 * User: marcuse
 * Date: Jan 15, 2009
 * Time: 3:19:11 PM
 * To change this template use File | Settings | File Templates.
 */
public abstract class AbstractPacket {
    private int length;
    private byte[] buffer;
    private int bufferPointer=0;

    /**
     * Creates a new packet, reads length from the first three bytes
     * Also buffers the whole packet.
     * @param reader the reader to use
     * @throws IOException if it was not possible to read from the inputstream of if there is a problem parsing the length
     */
    public AbstractPacket(InputStream reader) throws IOException {
        byte [] lengthBuffer = new byte[4];
        int readBytes = reader.read(lengthBuffer,0,4);
        if(readBytes!=4)
            throw new IOException("Could not read packet");
        this.length = lengthBuffer[0]+(lengthBuffer[1]<<8) + (lengthBuffer[2]<<16);
        buffer=new byte[this.length+1];
        readBytes = reader.read(buffer,0,this.length);
        if(readBytes != this.length)
            throw new IOException("Could not read packet");
    }

    /**
     * Reads a string from the buffer, looks for a 0 to end the string
     * @param charset the charset to use, for example ASCII
     * @return the read string
     * @throws IOException if it is not possible to create the string from the buffer
     */
    protected String readString(String charset) throws IOException {
        int startPos = bufferPointer;
        // find the end of the string
        while(bufferPointer < length && buffer[bufferPointer] != 0) {
            bufferPointer++;
        }
        String returnString = new String(buffer,startPos,bufferPointer,charset);
        if(bufferPointer < length)
            bufferPointer++; //read away the string-ending zero
        return returnString;
    }

    /**
     * read an integer (4 bytes) from the buffer;
     * @return an integer
     * @throws IOException if there are not 4 bytes left in the buffer
     */
    protected long readLong() throws IOException {
        if(length - bufferPointer < 4)
            throw new IOException("Could not read integer");
        return buffer[bufferPointer++] + (buffer[bufferPointer++]<<8) +(buffer[bufferPointer++]<<16) +(buffer[bufferPointer++]<<32);
    }

    /**
     * reads a byte from the buffer
     * @return the byte
     * @throws IOException if bufferPointer exceeds the length of the buffer
     */
    protected byte readByte() throws IOException {
        if(bufferPointer > length)
            throw new IOException("Could not read byte");
        return buffer[bufferPointer++];
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
}