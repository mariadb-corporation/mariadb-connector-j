package org.drizzle.jdbc.internal.packet;

import java.io.OutputStream;
import java.io.IOException;

/**
 .
 * User: marcuse
 * Date: Mar 25, 2009
 * Time: 9:37:59 PM

 */
public interface CommandPacket {
    public void send(OutputStream os) throws IOException;
}
