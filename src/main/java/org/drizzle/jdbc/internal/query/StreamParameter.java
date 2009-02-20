package org.drizzle.jdbc.internal.query;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * User: marcuse
 * Date: Feb 19, 2009
 * Time: 8:53:14 PM
 */
public class StreamParameter implements ParameterHolder{
    private final InputStream stream;
    private long length;

    public StreamParameter(InputStream is, long length) {
        stream=is;
        this.length = length;
    }

    public byte read() {
        try {
            return (byte)(stream.read());
        } catch (IOException e) {
            throw new RuntimeException("Not able to read stream");
        }
    }

    //    public byte read();
    public void writeTo(OutputStream os) throws IOException {
        for(int i=0;i<length;i++)
            os.write(stream.read());
    }

    public long length() {
        return length;
    }
}