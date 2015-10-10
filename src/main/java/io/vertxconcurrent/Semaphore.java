package io.vertxconcurrent;

import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;

import java.util.PriorityQueue;
import java.util.Queue;

/**
 * Created by Rob Worsnop on 10/10/15.
 */
public class Semaphore {
    private int availablePermits;
    private final Vertx vertx;
    private final Queue<DeferredContextAction> deferredActions = new PriorityQueue<>();

    public Semaphore(int permits, Vertx vertx) {
        availablePermits = permits;
        this.vertx = vertx;
    }
    public Semaphore(int permits, io.vertx.rxjava.core.Vertx vertx) {
        this(permits, (Vertx) vertx.getDelegate());
    }

    public void acquire(int permits, Handler<Void> action){
        ContextAction contextAction = new ContextAction(action, vertx.getOrCreateContext());

        synchronized (this){
            if (permits <= availablePermits){
                availablePermits -= permits;
                contextAction.run();
            } else{
                deferredActions.offer(new DeferredContextAction(permits, contextAction));
            }
        }
    }

    public void acquire(Handler<Void> action){
        acquire(1, action);
    }

    public synchronized boolean tryAcquire(int permits, Handler<Void> action){
        if (permits <= availablePermits){
            availablePermits -= permits;
            vertx.getOrCreateContext().runOnContext(action);
            return true;
        } else{
            return false;
        }
    }

    public synchronized void release(int permits){
        availablePermits += permits;
        DeferredContextAction action = deferredActions.peek();
        while (action != null && action.getPermits() <= availablePermits){
            availablePermits -= action.getPermits();
            action.getAction().run();
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

    private static class ContextAction {
        private final Handler<Void> action;
        private final Context context;

        public ContextAction(Handler<Void> action, Context context) {
            this.action = action;
            this.context = context;
        }

        public void run(){
            context.runOnContext(action);
        }
    }

    private static class DeferredContextAction implements Comparable<DeferredContextAction>{
        private final Integer permits;
        private final ContextAction action;

        public DeferredContextAction(int permits, ContextAction action) {
            this.permits = permits;
            this.action = action;
        }

        @Override
        public int compareTo(DeferredContextAction other) {
            return permits.compareTo(other.permits);
        }

        public ContextAction getAction() {
            return action;
        }

        public int getPermits() {
            return permits;
        }
    }

}
