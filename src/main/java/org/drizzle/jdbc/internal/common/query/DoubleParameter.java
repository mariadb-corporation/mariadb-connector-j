package org.drizzle.jdbc.internal.common.query;

import java.io.OutputStream;
import java.io.IOException;

/**
 .
 * User: marcuse
 * Date: Feb 27, 2009
 * Time: 10:00:38 PM

 */
public class DoubleParameter implements ParameterHolder {
    private final byte [] rawBytes;
    public DoubleParameter(double x) {
        rawBytes=String.valueOf(x).getBytes();
    }

    public void writeTo(OutputStream os) throws IOException {
        for(byte theByte:rawBytes)
            os.write(theByte);
    }

    public long length() {
        return rawBytes.length;
    }
}
