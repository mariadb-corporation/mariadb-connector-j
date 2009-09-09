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

/**
 * . User: marcuse Date: Feb 19, 2009 Time: 8:29:14 PM
 */
public interface ParameterHolder {
    void writeTo(OutputStream os) throws IOException;

    long length();
}
