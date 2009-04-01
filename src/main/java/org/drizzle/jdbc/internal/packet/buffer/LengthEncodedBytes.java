package org.drizzle.jdbc.internal.packet.buffer;

import java.util.Arrays;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Mar 31, 2009
 * Time: 6:51:36 PM
 * To change this template use File | Settings | File Templates.
 */
public class LengthEncodedBytes {
    private final byte [] theBytes;
    private final int length;
    public LengthEncodedBytes(byte[] rawBytes, int start) {
        if(start < rawBytes.length) {
            int tempLength;
            switch(rawBytes[start]&0xff) {
              case 251:
                  this.theBytes=null;
                  this.length=1;
                  break;
              case 252:
                  tempLength = ReadUtil.readShort(rawBytes, start+1);
                  this.theBytes = Arrays.copyOfRange(rawBytes,start+3,tempLength+start+3);
                  this.length=tempLength+theBytes.length+2;
                  break;
              case 253:
                  tempLength = ReadUtil.read24bitword(rawBytes, start+1);
                  this.theBytes = Arrays.copyOfRange(rawBytes,start+4,tempLength+start+4);
                  this.length=tempLength+theBytes.length+3;
                  break;
              case 254:
                  tempLength = (int)ReadUtil.readLong(rawBytes, start+1); // todo: yeah i need to fix this
                  this.theBytes = Arrays.copyOfRange(rawBytes,start+9,tempLength+start+9);
                  this.length=tempLength+theBytes.length+9;
                  break;
              default:
                  this.theBytes=Arrays.copyOfRange(rawBytes,start+1,start+1+(rawBytes[start]&0xff));
                  this.length=theBytes.length+1;
            }
        } else {
            theBytes=null;
            length=0;
        }
    }
    public byte[] getBytes() {
        return theBytes;
    }
    public int getLength() {
        return length;
    }


}
