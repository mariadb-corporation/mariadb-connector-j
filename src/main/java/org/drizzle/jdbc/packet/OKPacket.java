package org.drizzle.jdbc.packet;

import org.drizzle.jdbc.packet.buffer.ReadBuffer;

import java.io.InputStream;
import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Jan 16, 2009
 * Time: 4:23:40 PM
 * To change this template use File | Settings | File Templates.
 */
public class OKPacket extends ResultPacket {
    public OKPacket(ReadBuffer readBuffer) throws IOException {
        
    }

    public ResultType getResultType() {
        return ResultType.OK;
    }
}
