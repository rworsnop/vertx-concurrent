package io.vertxconcurrent;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.Timeout;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeoutException;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Created by Rob Worsnop on 10/10/15.
 */


@RunWith(VertxUnitRunner.class)
public class SemaphoreTest {

    @Rule
    public RunTestOnContext contextRule = new RunTestOnContext();

    @Rule
    public Timeout timeoutRule = new Timeout(20, MILLISECONDS);

    @Test
    public void simpleAcquire(TestContext context){
        Async async = context.async();
        Semaphore semaphore = new Semaphore(2, contextRule.vertx());
        semaphore.acquire(2, async::complete);
    }

    @Test
    public void acquireNeedingRelease(TestContext context){
        Async async = context.async();
        Semaphore semaphore = new Semaphore(-2, contextRule.vertx());
        semaphore.acquire(3, async::complete);
        semaphore.release(5);
    }

    @Test(expected = TimeoutException.class)
    public void acquireNotEnoughPermits(TestContext context){
        Async async = context.async();
        Semaphore semaphore = new Semaphore(1, contextRule.vertx());
        semaphore.acquire(2, async::complete);
    }

    @Test
    public void simpleSingleAcquireSinglePermit(TestContext context){
        Async async = context.async();
        Semaphore semaphore = new Semaphore(1, contextRule.vertx());
        semaphore.acquire(async::complete);
    }

    @Test
    public void acquireNeedingReleaseSinglePermit(TestContext context){
        Async async1 = context.async();
        Async async2 = context.async();
        Semaphore semaphore = new Semaphore(1, contextRule.vertx());
        semaphore.acquire(async1::complete);
        semaphore.acquire(async2::complete);
        semaphore.release();
    }

    @Test
    public void tryAcquire(TestContext context){
        Semaphore semaphore = new Semaphore(1, contextRule.vertx());
        context.assertTrue(semaphore.tryAcquire());
        context.assertEquals(0, semaphore.getAvailablePermits());
    }

    @Test
    public void tryAcquireNotEnoughPermits(TestContext context){
        Semaphore semaphore = new Semaphore(1, contextRule.vertx());
        context.assertFalse(semaphore.tryAcquire(2));
    }

    @Test
    public void tryAcquireWithTimeoutNoWait(TestContext context){
        Async async = context.async();
        Semaphore semaphore = new Semaphore(1, contextRule.vertx());
        semaphore.tryAcqure(1, MILLISECONDS, result -> {
            context.assertTrue(result);
            async.complete();
        });
    }

    @Test
    public void tryAcquireWithTimeoutNeedWait(TestContext context){
        Async async = context.async();
        Semaphore semaphore = new Semaphore(1, contextRule.vertx());
        semaphore.tryAcquire(2, 20, MILLISECONDS, result->{
            context.assertTrue(result);
            async.complete();
        });
        contextRule.vertx().setTimer(1, v -> semaphore.release());
    }

    @Test
    public void tryAcquireWithTimeoutFailing(TestContext context){
        Async async = context.async();
        Semaphore semaphore = new Semaphore(1, contextRule.vertx());
        semaphore.tryAcquire(2, 5, MILLISECONDS, result -> {
            context.assertFalse(result);
            async.complete();
        });
    }

    @Test
    public void drainPermits(TestContext context){
        Semaphore semaphore = new Semaphore(1, contextRule.vertx());
        context.assertEquals(1, semaphore.drainPermits());
        context.assertEquals(0, semaphore.getAvailablePermits());
        context.assertFalse(semaphore.tryAcquire(1));
    }

    @Test
    public void getQueueLength(TestContext context){
        Semaphore semaphore = new Semaphore(9, contextRule.vertx());
        semaphore.acquire(10, () -> {
        });
        semaphore.acquire(10, () -> {
        });
        context.assertEquals(2, semaphore.getQueueLength());
    }

    @Test
    public void getAvailablePermits(TestContext context){
        Semaphore semaphore = new Semaphore(100, contextRule.vertx());
        context.assertEquals(100, semaphore.getAvailablePermits());
    }

    @Test
    public void testFairness(TestContext context){
        Async async = context.async();
        Semaphore semaphore = new Semaphore(1, true, contextRule.vertx());
        semaphore.acquire(100, async::complete);
        semaphore.acquire(2, ()->{});
        semaphore.release(100);

    }

    @Test
    public void testUnfairness(TestContext context){
        Async async = context.async();
        Semaphore semaphore = new Semaphore(1, false, contextRule.vertx());
        semaphore.acquire(100, ()->{});
        semaphore.acquire(2, async::complete);
        semaphore.release(100);

    }
}
