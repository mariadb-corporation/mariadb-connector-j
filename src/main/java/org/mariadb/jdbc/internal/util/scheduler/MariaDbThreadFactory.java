package org.mariadb.jdbc.internal.util.scheduler;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class MariaDbThreadFactory implements ThreadFactory {
    private static final AtomicInteger POOL_ID = new AtomicInteger();
    
    private final int thisPoolId = POOL_ID.incrementAndGet();
    // start from DefaultThread factory to get security groups and what not
    private final ThreadFactory parentFactory = Executors.defaultThreadFactory();
    private final AtomicInteger threadId = new AtomicInteger();

    @Override
    public Thread newThread(Runnable runnable) {
        Thread result = parentFactory.newThread(runnable);
        result.setName("mariaDb-" + thisPoolId + "-" + threadId.incrementAndGet());
        result.setDaemon(true); // set as daemon so that mariaDb wont hold up shutdown
        
        return result;
    }
}
