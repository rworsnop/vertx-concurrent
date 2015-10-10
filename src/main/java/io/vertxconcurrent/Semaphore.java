package io.vertxconcurrent;

import io.vertx.core.Context;
import io.vertx.core.Vertx;

import java.util.LinkedList;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Created by Rob Worsnop on 10/10/15.
 */
public class Semaphore {
    private int availablePermits;
    private final Vertx vertx;
    private final Queue<DeferredContextAction> deferredActions;

    public Semaphore(int permits, Vertx vertx) {
        this(permits, false, vertx);
    }

    public Semaphore(int permits, boolean fair, Vertx vertx) {
        availablePermits = permits;
        this.vertx = vertx;
        deferredActions = fair? new LinkedList<>() : new PriorityQueue<>();
    }

    public Semaphore(int permits, io.vertx.rxjava.core.Vertx vertx) {
        this(permits, (Vertx) vertx.getDelegate());
    }

    public Semaphore(int permits, boolean fair, io.vertx.rxjava.core.Vertx vertx) {
        this(permits, fair, (Vertx) vertx.getDelegate());
    }

    public void acquire(int permits, Runnable action){
        synchronized (this){
            if (permits <= availablePermits){
                availablePermits -= permits;
                runOnContext(action);
            } else{
                deferredActions.offer(new DeferredContextAction(permits, action, vertx.getOrCreateContext()));
            }
        }
    }

    public void acquire(Runnable action){
        acquire(1, action);
    }

    public synchronized boolean tryAcquire(int permits, Runnable action){
        if (permits <= availablePermits){
            availablePermits -= permits;
            runOnContext(action);
            return true;
        } else{
            return false;
        }
    }

    public void tryAcquire(int permits, long timeout, TimeUnit unit, Consumer<Boolean> resultHandler){
        Runnable successAction = ()->resultHandler.accept(true);

        synchronized (this){
            if (permits <= availablePermits){
                availablePermits -= permits;
                runOnContext(successAction);
            } else{
                TimerCancellingAction timerCancellingAction = new TimerCancellingAction(successAction);
                DeferredContextAction deferredAction = new DeferredContextAction(permits, timerCancellingAction, vertx.getOrCreateContext());
                long timerId = vertx.setTimer(unit.toMillis(timeout), id -> timeout(deferredAction, resultHandler));
                timerCancellingAction.setTimerId(timerId);
                deferredActions.offer(deferredAction);
            }
        }
    }

    public void tryAcqure(long timeout, TimeUnit unit, Consumer<Boolean> resultHandler){
        tryAcquire(1, timeout, unit, resultHandler);
    }

    public synchronized void release(int permits){
        availablePermits += permits;
        DeferredContextAction action = deferredActions.peek();
        while (action != null && action.getPermits() <= availablePermits){
            availablePermits -= action.getPermits();
            action.run();
            deferredActions.remove();
            action = deferredActions.peek();
        }
    }

    public void release(){
        release(1);
    }

    public synchronized int drainPermits(){
        int permits = availablePermits;
        availablePermits = 0;
        return permits;
    }

    public int getQueueLength(){
        return deferredActions.size();
    }

    public int getAvailablePermits() {
        return availablePermits;
    }

    private void timeout(DeferredContextAction deferredAction, Consumer<Boolean> resultHandler){
        synchronized (this){
            if (deferredActions.remove(deferredAction)){
                // if there was nothing to remove, action must already have been taken by release()
                runOnContext(()->resultHandler.accept(false));
            }
        }
    }

    private  class TimerCancellingAction implements Runnable{
        final Runnable delegate;
        Long timerId;

        public TimerCancellingAction(Runnable delegate) {
            this.delegate = delegate;
        }

        @Override
        public void run() {
            Objects.requireNonNull(timerId);
            vertx.cancelTimer(timerId);
            delegate.run();
        }

        public void setTimerId(Long timerId) {
            this.timerId = timerId;
        }
    }

    private static class DeferredContextAction implements Comparable<DeferredContextAction>{
        private final Integer permits;
        private final Runnable action;
        private final Context context;

        public DeferredContextAction(int permits, Runnable action, Context context) {
            this.permits = permits;
            this.action = action;
            this.context = context;
        }

        @Override
        public int compareTo(DeferredContextAction other) {
            return permits.compareTo(other.permits);
        }

        public void run(){
            context.runOnContext(v->action.run());
        }


        public int getPermits() {
            return permits;
        }
    }

    private void runOnContext(Runnable action){
        vertx.getOrCreateContext().runOnContext(v->action.run());
    }

}
