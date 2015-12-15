package org.mariadb.jdbc.internal.util;

import static org.junit.Assert.*;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mariadb.jdbc.internal.util.SchedulerServiceProviderHolder.SchedulerProvider;
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
        assertTrue(SchedulerServiceProviderHolder.DEFAULT_PROVIDER 
                       == SchedulerServiceProviderHolder.getSchedulerProvider());
    }
    
    @Test
    public void defaultProviderGetSchedulerTest() {
        SchedulerProvider provider = SchedulerServiceProviderHolder.getSchedulerProvider();
        ScheduledExecutorService scheduler = provider.getScheduler(1);
        try {
            assertNotNull(scheduler);
            // verify scheduler works
            TestRunnable tr = new TestRunnable();
            scheduler.execute(tr);
            tr.blockTillFinished(); // will throw exception if timeout
        } finally {
            provider.shutdownScheduler(scheduler);
        }
    }
    
    @Test
    public void defaultProviderSchedulerShutdownTest() {
        SchedulerProvider provider = SchedulerServiceProviderHolder.getSchedulerProvider();
        ScheduledExecutorService scheduler = provider.getScheduler(1);
        
        provider.shutdownScheduler(scheduler);
        
        try {
            scheduler.execute(DoNothingRunnable.instance());
            
            fail("Exception should have thrown");
        } catch (RejectedExecutionException expected) {
            // ignore
        }
    }
    
    @Test
    public void defaultProviderSchedulerShutdownFail() {
        SchedulerProvider provider = SchedulerServiceProviderHolder.getSchedulerProvider();
        ScheduledExecutorService scheduler = provider.getScheduler(1);
        
        try {
            try {
                scheduler.shutdown();
                fail("Exception should have thrown");
            } catch (UnsupportedOperationException expected) {
                // ignore
            }
            assertFalse(scheduler.isShutdown());
            try {
                scheduler.shutdownNow();
                fail("Exception should have thrown");
            } catch (UnsupportedOperationException expected) {
                // ignore
            }
            assertFalse(scheduler.isShutdown());
        } finally {
            provider.shutdownScheduler(scheduler);
        }
    }
    
    @Test
    public void setAndGetProviderTest() {
        SchedulerProvider emptyProvider = new SchedulerProvider() {
            @Override
            public ScheduledExecutorService getScheduler(int minimumThreads) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void shutdownScheduler(ScheduledExecutorService scheduler) {
                throw new UnsupportedOperationException();
            }
        };
        
        SchedulerServiceProviderHolder.setSchedulerProvider(emptyProvider);
        assertTrue(emptyProvider == SchedulerServiceProviderHolder.getSchedulerProvider());
    }
}
