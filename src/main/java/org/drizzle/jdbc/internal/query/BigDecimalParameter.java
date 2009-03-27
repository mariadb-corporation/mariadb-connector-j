package org.drizzle.jdbc.internal.query;

import java.math.BigDecimal;
import java.io.OutputStream;
import java.io.IOException;

/**
 * User: marcuse
 * Date: Feb 27, 2009
 * Time: 10:07:00 PM

 */
public class BigDecimalParameter implements ParameterHolder {
    private final byte[] rawBytes;

    public BigDecimalParameter(BigDecimal x) {
        this.rawBytes = x.toPlainString().getBytes();
    }

    public void writeTo(OutputStream os) throws IOException {
        for(byte theByte : rawBytes)
            os.write(theByte);
    }

    public long length() {
        return rawBytes.length;
    }
}
