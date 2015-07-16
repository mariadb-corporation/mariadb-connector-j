package org.mariadb.jdbc.failover;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.Executors;
import java.util.concurrent.RunnableScheduledFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class TcpProxy {
    protected static Logger log = Logger.getLogger("org.maria.jdbc");

    String host;
    int remoteport;
    TcpProxySocket socket;

    public TcpProxy(String host, int remoteport) throws IOException {
        this.host = host;
        this.remoteport = remoteport;
        socket = new TcpProxySocket(host, remoteport);
        Executors.newSingleThreadScheduledExecutor().schedule(socket, 0, TimeUnit.MILLISECONDS);
    }

    public void restart(long sleepTime) {
        socket.kill();
        Executors.newSingleThreadScheduledExecutor().schedule(socket, sleepTime, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        socket.kill();
    }

    public void restart() {
        Executors.newSingleThreadExecutor().execute(socket);
        try {
            Thread.sleep(10);
        }catch(InterruptedException e) {}
    }
    public void assureProxyOk() {
        if (socket.isClosed()) {
            restart();
        }
    }

    public int getLocalPort() {
        return socket.getLocalPort();
    }

}
