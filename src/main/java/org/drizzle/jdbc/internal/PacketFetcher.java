package org.drizzle.jdbc.internal;

import org.drizzle.jdbc.internal.packet.RawPacket;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.io.InputStream;
import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Mar 31, 2009
 * Time: 2:01:30 PM
 * To change this template use File | Settings | File Templates.
 */
public class PacketFetcher implements Runnable {
    private final BlockingQueue<RawPacket> packet = new LinkedBlockingQueue<RawPacket>();
    private final InputStream inputStream;
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private volatile boolean shutDown=false;
    public PacketFetcher(InputStream inputStream) {
        this.inputStream = inputStream;
        executorService.submit(this);
    }

    public void run() {
        while(!shutDown) {
            try {
                RawPacket rawPacket = new RawPacket(inputStream);
                packet.add(rawPacket);
            } catch (IOException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }
        try {
            inputStream.close();
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    public RawPacket getRawPacket() {
        try {
            return packet.take();
        } catch (InterruptedException e) {
            throw new RuntimeException("doh",e); //Todo: fix
        }
    }

    public void shutdown() {
        this.shutDown=true;
    }
}
