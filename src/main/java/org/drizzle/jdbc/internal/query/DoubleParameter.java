package org.drizzle.jdbc.internal.query;

import java.io.OutputStream;
import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Feb 27, 2009
 * Time: 10:00:38 PM
 * To change this template use File | Settings | File Templates.
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
        return rawBytes.length;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
