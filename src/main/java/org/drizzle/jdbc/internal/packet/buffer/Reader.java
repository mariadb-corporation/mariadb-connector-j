package org.drizzle.jdbc.internal.packet.buffer;

import java.io.InputStream;
import java.io.IOException;
import java.io.BufferedInputStream;

/**
 .
 * User: marcuse
 * Date: Jan 16, 2009
 * Time: 8:27:38 PM

 */
public class Reader {
    private final InputStream reader;
    private final int length;
    private int readBytes;
    private final byte packetSeq;

    public Reader(InputStream reader) throws IOException {
        this.reader=reader;
        this.length=readLength();
        this.packetSeq = readPacketSeq();
    }
    private int readLength() throws IOException {
        byte [] lengthBuffer = new byte[3];
        for(int i =0 ;i<3;i++)
            lengthBuffer[i]= (byte) reader.read();
        return  (lengthBuffer[0] & 0xff)+(lengthBuffer[1]<<8) + (lengthBuffer[2]<<16);
    }

    private  byte readPacketSeq() throws IOException {
        return (byte)reader.read();
    }


    /**
     * Reads a string from the buffer, looks for a 0 to end the string
     * @param charset the charset to use, for example ASCII
     * @return the read string
     * @throws java.io.IOException if it is not possible to create the string from the buffer
     */
    public  String readString(String charset) throws IOException {
        int ch=0;
        byte [] tempArr = new byte[length-readBytes]; //todo: fix!
        int i=0;
        while(readBytes < length && ((ch = reader.read()) != 0) ) {
            readBytes++;
            tempArr[i++]=(byte)ch;
        }
        return new String(tempArr,charset);
    }
   /**
     * read a short (2 bytes) from the buffer;
     * @return an short
     * @throws java.io.IOException if there are not 2 bytes left in the buffer
     */
    public  short readShort() throws IOException {
        if(readBytes+2>length)
            throw new IOException("Could not read short");
        byte[] tempBuf = new byte[2];
        tempBuf[0]= (byte) reader.read();
        tempBuf[1]=(byte)reader.read();
        readBytes+=2;
        return (short) (tempBuf[0] + (tempBuf[1]<<8));
    }

    /**
     * read a int (4 bytes) from the buffer;
     * @return a int
     * @throws java.io.IOException if there are not 4 bytes left in the buffer
     */
    public  int readInt() throws IOException {
        if(readBytes+4>length)
            throw new IOException("Could not read int");

        byte[] tempBuf = new byte[4];
        for(int i=0;i<4;i++)
            tempBuf[i]= (byte) reader.read();
        readBytes+=4;
        return tempBuf[0] + (tempBuf[1]<<8) +(tempBuf[2]<<16) +(tempBuf[3]<<24);
    }

    /**
     * read a long (8 bytes) from the buffer;
     * @return a long
     * @throws java.io.IOException if there are not 8 bytes left in the buffer
     */
    public  long readLong() throws IOException {
        if(readBytes+8>length)
            throw new IOException("Could not read short");

        byte[] tempBuf = new byte[8];
        for(int i=0;i<8;i++)
            tempBuf[i]= (byte) reader.read();
        readBytes+=8;
        return
                tempBuf[0] +
               (tempBuf[1]<<8) +
               (tempBuf[2]<<16) +
               (tempBuf[3]<<24) +
               (tempBuf[4]<<32)+
               (tempBuf[5]<<40)+
               (tempBuf[6]<<48) +
               (tempBuf[7]<<56);
    }


    /**
     * reads a byte from the buffer
     * @return the byte
     * @throws java.io.IOException if bufferPointer exceeds the length of the buffer
     */
    public  byte readByte() throws IOException {
        if(readBytes>=length)
            throw new IOException("Could not read byte");
        readBytes++;
        return (byte)reader.read();
    }
    public  byte[] readRawBytes(int numberOfBytes) throws IOException {
        if(readBytes+numberOfBytes >= length)
            throw new IOException("Could not read bytes");
        byte [] tmpArr = new byte[numberOfBytes];
        for(int i=0;i<numberOfBytes;i++){
            tmpArr[i] = (byte) reader.read();
        }
        readBytes+=numberOfBytes;
        return tmpArr;
    }
    public  void skipByte() throws IOException {
        skipBytes(1);
    }

    public long skipBytes(int bytesToSkip) throws IOException {
        if(readBytes+bytesToSkip>length)
            throw new IOException("Could not skip bytes");
        readBytes+=bytesToSkip;
        return reader.skip(bytesToSkip);

    }

    public  int read24bitword() throws IOException {
        if(readBytes+3>=length)
            throw new IOException("Could not read 3 bytes");
        byte [] tmpArr = new byte[3];
        for(int i =0 ; i<3;i++)
            tmpArr[i]=(byte)reader.read();
        readBytes+=3;
        return tmpArr[0] + (tmpArr[1]<<8) +(tmpArr[2]<<16);
    }

    public  long getLengthEncodedBinary() throws IOException {
        if(readBytes>=length)
            throw new IOException("Could not read length encoded binary ("+readBytes+")("+length+")");
        byte type = (byte) reader.read();
        readBytes+=1;

        if(type == (byte)251)
            return -1;
        if(type == (byte)252)
            return (long) readShort();
        if(type == (byte)253)
            return read24bitword();
        if(type == (byte)254) {
            return readLong();
        }
        if(type <= 250)
            return (long)type;

        return 0;
    }

    public  byte peek() throws IOException {
        reader.mark(2);
        byte b = (byte) reader.read();
        reader.reset();
        return b;
    }
    public  String getLengthEncodedString() throws IOException {
        long encLength = getLengthEncodedBinary();
        if(encLength==-1) return null;
        if(readBytes+encLength > length)
            throw new IOException("Could not read length encoded binary ("+readBytes+")("+encLength+")("+length+")");
        byte [] tmpBuf = new byte[(int)encLength];
        for(int i=0;i<encLength;i++)
            tmpBuf[i] = (byte) reader.read();
        readBytes+=encLength;
        return new String(tmpBuf, "ASCII");
    }

    public byte[] getLengthEncodedBytes() throws IOException {
        long encLength = getLengthEncodedBinary();
        if(encLength==-1) return null;
        if(readBytes+encLength > length)
            throw new IOException("Could not read length encoded binary ("+readBytes+")("+encLength+")("+length+")");
        byte [] tmpBuf = new byte[(int)encLength];
        for(int i=0;i<encLength;i++)
            tmpBuf[i] = (byte) reader.read();
        readBytes+=encLength;
        return tmpBuf;
    }

    public  byte getByteAt(int i) throws IOException {
        reader.mark(i+1);
        reader.skip(i-1);
        byte b = (byte) reader.read();
        reader.reset();
        return b;
    }

    public byte getPacketSeq() {
        return packetSeq;
    }
}