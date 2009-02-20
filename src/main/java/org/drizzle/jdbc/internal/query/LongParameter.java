package org.drizzle.jdbc.internal.query;

import org.drizzle.jdbc.internal.query.ParameterHolder;

import java.io.OutputStream;
import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Feb 19, 2009
 * Time: 8:50:52 PM
 * To change this template use File | Settings | File Templates.
 */
public class LongParameter implements ParameterHolder {
    private final byte [] byteRepresentation;
    private byte bytePointer=0;

    public LongParameter(long theLong) {
        byteRepresentation = String.valueOf(theLong).getBytes();
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
        return byteRepresentation.length;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
