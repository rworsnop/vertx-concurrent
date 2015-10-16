package io.vertxconcurrent;

import io.vertx.core.Vertx;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Created by Rob Worsnop on 10/11/15.
 */
public class CountDownLatch {

    private final AtomicInteger count;
    private final Vertx vertx;

    private final List<ContextAction> deferredActions = new LinkedList<>();

    public CountDownLatch(int count, Vertx vertx) {
        this.count = new AtomicInteger(count);
        this.vertx = vertx;
    }

    public CountDownLatch(int count, io.vertx.rxjava.core.Vertx vertx){
        this(count, (Vertx) vertx.getDelegate());
    }

    public void countDown(){
        int was = count.getAndUpdate(operand -> operand > 0 ? operand - 1 : 0);
        if (was == 1){
            synchronized (deferredActions){
                deferredActions.stream().forEach(ContextAction::run);
                deferredActions.clear();
            }
        }
    }

    public void await(Runnable action){
        if (count.get() == 0){
            vertx.runOnContext(v -> action.run());
        } else{
            ContextAction contextAction = new ContextAction(action, vertx.getOrCreateContext());
            synchronized (deferredActions){
                deferredActions.add(contextAction);
            }
        }
    }

    public void await(long timeout, TimeUnit unit, Consumer<Boolean> resultHandler){
        Runnable successAction = ()->resultHandler.accept(true);
        if (count.get() == 0){
            runOnContext(successAction);
        } else{
            TimerCancellingAction timerCancellingAction = new TimerCancellingAction(vertx, successAction);
            ContextAction contextAction = new ContextAction(timerCancellingAction, vertx.getOrCreateContext());
            long timerId = vertx.setTimer(unit.toMillis(timeout), id -> timeout(contextAction, resultHandler));
            timerCancellingAction.setTimerId(timerId);
            deferredActions.add(contextAction);
        }
    }

    public int getCount(){
        return count.get();
    }

    private void runOnContext(Runnable action){
        vertx.getOrCreateContext().runOnContext(v->action.run());
    }

    private void timeout(ContextAction deferredAction, Consumer<Boolean> resultHandler){
        synchronized (this){
            if (deferredActions.remove(deferredAction)){
                // if there was nothing to remove, action must already have been taken by release()
                runOnContext(()->resultHandler.accept(false));
            }
        }
    }
}
