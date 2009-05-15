/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc.internal.drizzle.packet;

import org.drizzle.jdbc.internal.drizzle.QueryException;

import java.io.OutputStream;
import java.io.IOException;

/**
 .
 * User: marcuse
 * Date: Mar 25, 2009
 * Time: 9:37:59 PM

 */
public interface CommandPacket {
    public void send(OutputStream os) throws IOException, QueryException;
}
