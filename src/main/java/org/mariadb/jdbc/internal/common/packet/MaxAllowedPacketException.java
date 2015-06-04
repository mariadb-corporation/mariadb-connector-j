package org.mariadb.jdbc.internal.common.packet;

import java.io.IOException;

/**
 * Created by diego_000 on 03/06/2015.
 */
public class MaxAllowedPacketException extends IOException {
    boolean mustReconnect;
    public MaxAllowedPacketException(String message, boolean mustReconnect) {
        super(message);
        this.mustReconnect = mustReconnect;
    }

    public boolean isMustReconnect() {
        return mustReconnect;
    }
}
