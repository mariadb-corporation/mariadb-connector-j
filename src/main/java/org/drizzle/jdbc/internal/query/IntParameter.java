package org.drizzle.jdbc.internal.query;

import java.io.OutputStream;
import java.io.IOException;

/**
 .
 * User: marcuse
 * Date: Feb 19, 2009
 * Time: 8:48:15 PM

 */
public class IntParameter implements ParameterHolder{
    private final byte [] byteRepresentation;
    private byte bytePointer=0;
    public IntParameter(int theInt) {
        byteRepresentation = String.valueOf(theInt).getBytes();
    }

    public byte read() {
        if(bytePointer<byteRepresentation.length) {
            return byteRepresentation[bytePointer++];
        }
        return -1;
    }

    public void writeTo(OutputStream os) throws IOException {
        for(byte b:byteRepresentation)
            os.write(b);
    }

    public long length() {
        return byteRepresentation.length;
    }
}
