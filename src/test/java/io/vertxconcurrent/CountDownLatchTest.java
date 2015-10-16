package io.vertxconcurrent;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.Timeout;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Created by Rob Worsnop on 10/15/15.
 */
@RunWith(VertxUnitRunner.class)
public class CountDownLatchTest {
    @Rule
    public RunTestOnContext contextRule = new RunTestOnContext();

    @Rule
    public Timeout timeoutRule = new Timeout(20, MILLISECONDS);

    @Test
    public void await(TestContext context){
        Async async1 = context.async();
        Async async2 = context.async();

        CountDownLatch latch = new CountDownLatch(3, contextRule.vertx());

        latch.await(async1::complete);
        latch.await(async2::complete);

        latch.countDown();
        latch.countDown();
        latch.countDown();
    }

    @Test
    public void awaitAlreadyZero(TestContext context){
        Async async = context.async();
        CountDownLatch latch = new CountDownLatch(0, contextRule.vertx());
        latch.await(async::complete);
    }

    @Test
    public void awaitWithTimeoutNeedWait(TestContext context){
        Async async = context.async();

        CountDownLatch latch = new CountDownLatch(1, contextRule.vertx());

        latch.await(10, MILLISECONDS, result -> {
            context.assertTrue(result);
            async.complete();
        });

       contextRule.vertx().setTimer(1, id -> latch.countDown());
    }

    @Test
    public void awaitWithTimeoutFailing(TestContext context){
        Async async = context.async();

        CountDownLatch latch = new CountDownLatch(1, contextRule.vertx());

        latch.await(1, MILLISECONDS, result -> {
            context.assertFalse(result);
            async.complete();
        });
    }

    @Test
    public void awaitAlreadyZeroWithTimeout(TestContext context){
        Async async = context.async();
        CountDownLatch latch = new CountDownLatch(0, contextRule.vertx());
        latch.await(1, MILLISECONDS, result -> {
            context.assertTrue(result);
            async.complete();
        });
   }
}
