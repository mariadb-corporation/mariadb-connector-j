package org.mariadb.jdbc.internal.util.scheduler;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class MariaDbThreadFactory implements ThreadFactory {

    // start from DefaultThread factory to get security groups and what not
    private final ThreadFactory parentFactory = Executors.defaultThreadFactory();
    private final AtomicInteger threadId = new AtomicInteger();
    private final String poolName;

    public MariaDbThreadFactory(String poolName) {
        this.poolName = poolName;
    }

    @Override
    public Thread newThread(Runnable runnable) {
        Thread result = parentFactory.newThread(runnable);
        result.setName("MariaDb-" + poolName + "-" + threadId.incrementAndGet());
        result.setDaemon(true); // set as daemon so that mariaDb wont hold up shutdown

        return result;
    }
}
