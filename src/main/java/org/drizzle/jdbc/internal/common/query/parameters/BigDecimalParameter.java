/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc.internal.common.query.parameters;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;

/**
 * User: marcuse Date: Feb 27, 2009 Time: 10:07:00 PM
 */
public class BigDecimalParameter implements ParameterHolder {
    private final byte[] rawBytes;

    public BigDecimalParameter(final BigDecimal x) {
        this.rawBytes = x.toPlainString().getBytes();
    }

    public int writeTo(final OutputStream os, int offset, int maxWriteSize) throws IOException {
        int bytesToWrite = Math.min(rawBytes.length - offset, maxWriteSize);
        os.write(rawBytes, offset, bytesToWrite);
        return bytesToWrite;
    }

    public long length() {
        return rawBytes.length;
    }
}
