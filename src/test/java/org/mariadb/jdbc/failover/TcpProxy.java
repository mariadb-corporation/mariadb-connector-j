package org.mariadb.jdbc.failover;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


public class TcpProxy {
    protected static final Logger log = LoggerFactory.getLogger(TcpProxy.class);

    String host;
    int remoteport;
    TcpProxySocket socket;

    /**
     * Initialise procy.
     * @param host host (ip / dns)
     * @param remoteport port
     * @throws IOException  exception
     */
    public TcpProxy(String host, int remoteport) throws IOException {
        this.host = host;
        this.remoteport = remoteport;
        socket = new TcpProxySocket(host, remoteport);
        Executors.newSingleThreadScheduledExecutor().schedule(socket, 0, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        socket.kill();
    }

    public void restart(long sleepTime) {
        socket.kill();
        Executors.newSingleThreadScheduledExecutor().schedule(socket, sleepTime, TimeUnit.MILLISECONDS);
    }


    /**
     * Restart proxy.
     */
    public void restart() {
        Executors.newSingleThreadExecutor().execute(socket);
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            //eat Exception
        }
    }

    /**
     * Assure that proxy is in a stable status.
     */
    public void assureProxyOk() {
        if (socket.isClosed()) {
            restart();
        }
    }

    public int getLocalPort() {
        return socket.getLocalPort();
    }

}
