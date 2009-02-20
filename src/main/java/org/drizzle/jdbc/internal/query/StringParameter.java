package org.drizzle.jdbc.internal.query;

import static org.drizzle.jdbc.internal.Utils.sqlEscapeString;

import java.io.OutputStream;
import java.io.IOException;

/**
 * User: marcuse
 * Date: Feb 18, 2009
 * Time: 10:17:14 PM
 */
public class StringParameter implements ParameterHolder {
    private final byte [] byteRepresentation;
    private int bytePointer = 0;

    public StringParameter(String parameter) {
        String tempParam = "\""+sqlEscapeString(parameter)+"\"";
        this.byteRepresentation=tempParam.getBytes();
    }

    /**
     * Reads the next byte of data from the input stream. The value byte is
     * returned as an <code>int</code> in the range <code>0</code> to
     * <code>255</code>. If no byte is available because the end of the stream
     * has been reached, the value <code>-1</code> is returned. This method
     * blocks until input data is available, the end of the stream is detected,
     * or an exception is thrown.
     * <p/>
     * <p> A subclass must provide an implementation of this method.
     *
     * @return the next byte of data, or <code>-1</code> if the end of the
     *         stream is reached.
     */
    public byte read() {
        if(bytePointer<byteRepresentation.length){
//            System.out.printf("RETURNING: (%d = %c)",byteRepresentation[bytePointer],(char)byteRepresentation[bytePointer]);
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
