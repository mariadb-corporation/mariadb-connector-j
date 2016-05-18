package org.mariadb.jdbc.internal.util;

import static org.junit.Assert.*;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mariadb.jdbc.internal.util.scheduler.DynamicSizedSchedulerInterface;
import org.mariadb.jdbc.internal.util.scheduler.SchedulerServiceProviderHolder;
import org.mariadb.jdbc.internal.util.scheduler.SchedulerServiceProviderHolder.SchedulerProvider;
import org.threadly.concurrent.DoNothingRunnable;
import org.threadly.test.concurrent.TestRunnable;

public class SchedulerServiceProviderHolderTest {
    @After
    @Before
    public void providerReset() {
        SchedulerServiceProviderHolder.setSchedulerProvider(null);
    }

    @Test
    public void getDefaultProviderTest() {
        assertTrue(SchedulerServiceProviderHolder.DEFAULT_PROVIDER == SchedulerServiceProviderHolder.getSchedulerProvider());
    }

    @Test
    public void defaultProviderGetSchedulerTest() {
        testRunnable(SchedulerServiceProviderHolder.getScheduler(1));
        testRunnable(SchedulerServiceProviderHolder.getFixedSizeScheduler(1));
    }

    private void testRunnable(ScheduledExecutorService scheduler) {
        try {
            assertNotNull(scheduler);
            // verify scheduler works
            TestRunnable tr = new TestRunnable();
            scheduler.execute(tr);
            tr.blockTillFinished(); // will throw exception if timeout
        } finally {
            scheduler.shutdown();
        }
    }

    @Test
    public void defaultProviderSchedulerShutdownTest() {
        testExecuteAfterShutdown(SchedulerServiceProviderHolder.getScheduler(1));
        testExecuteAfterShutdown(SchedulerServiceProviderHolder.getFixedSizeScheduler(1));
    }

    private void testExecuteAfterShutdown(ScheduledExecutorService scheduler) {
        scheduler.shutdown();
        try {
            scheduler.execute(DoNothingRunnable.instance());
            fail("Exception should have thrown");
        } catch (RejectedExecutionException expected) {
            // ignore
        }
    }

    @Test
    public void setAndGetProviderTest() {
        SchedulerProvider emptyProvider = new SchedulerProvider() {
            @Override
            public DynamicSizedSchedulerInterface getScheduler(int minimumThreads) {
                throw new UnsupportedOperationException();
            }

            @Override
            public ScheduledThreadPoolExecutor getFixedSizeScheduler(int minimumThreads) {
                throw new UnsupportedOperationException();
            }

            @Override
            public ScheduledThreadPoolExecutor getTimeoutScheduler() {
                throw new UnsupportedOperationException();
            }
        };

        SchedulerServiceProviderHolder.setSchedulerProvider(emptyProvider);
        assertTrue(emptyProvider == SchedulerServiceProviderHolder.getSchedulerProvider());
    }
}
