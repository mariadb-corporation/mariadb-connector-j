package org.drizzle.jdbc.internal.query;

import java.io.OutputStream;
import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Feb 27, 2009
 * Time: 9:56:17 PM
 * To change this template use File | Settings | File Templates.
 */
public class ByteParameter implements ParameterHolder {
    private final byte[] bytes;

    public ByteParameter(byte[] x) {
        this.bytes=x;
    }


    public void writeTo(OutputStream os) throws IOException {
        for (byte aByte : bytes) {
            os.write(aByte);
        }
    }

    public long length() {
        return bytes.length;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
