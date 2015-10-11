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
 * <p>A counting semaphore.  Conceptually, a semaphore maintains a set of
 * permits.  Each {@link #acquire} waits if necessary until a permit is
 * available, and then executes the provided callback.  Each {@link #release} adds a permit,
 * potentially releasing a waiting acquirer. By "wait", it is not meant that anything is blocking. In fact,
 * {@link #acquire} will always return immediately, after either asynchronously executing the callback or
 * enqueing it for later.
 *
 * <p>Semaphores are thread-safe and can be shared between different event loops and worker threads. When
 * a callback is executed, it always runs on the same context as its {@link #acquire} call, even if it
 * is triggered from a {@link #release} call from a different thread.
 *
 * <p>By default, permits will be released in an optimal order, meaning that a request for 10 permits will be
 * given precedence over a request for 20 permits. The constructor for this class optionally accepts a
 * <em>fairness</em> parameter. When this set is set to true, permits will be released according to the order
 * in which they are acquired.
 *
 * @author Rob Worsnop
 */
public class Semaphore {
    private int availablePermits;
    private final Vertx vertx;
    private final Queue<DeferredContextAction> deferredActions;

    /**
     * Creates a {@code Semaphore} with the given number of
     * permits and nonfair fairness setting.
     *
     * @param permits the initial number of permits available.
     *        This value may be negative, in which case releases
     *        must occur before any acquires will be granted.
     * @param vertx the Vertx instance.
     */
    public Semaphore(int permits, Vertx vertx) {
        this(permits, false, vertx);
    }

    /**
     * Creates a {@code Semaphore} with the given number of
     * permits and the given fairness setting.
     *
     * @param permits the initial number of permits available.
     *        This value may be negative, in which case releases
     *        must occur before any acquires will be granted.
     * @param fair {@code true} if this semaphore will guarantee
     *        first-in first-out granting of permits under contention,
     *        else {@code false}

     * @param vertx the Vertx instance.
     */
    public Semaphore(int permits, boolean fair, Vertx vertx) {
        availablePermits = permits;
        this.vertx = vertx;
        deferredActions = fair? new LinkedList<>() : new PriorityQueue<>();
    }

    /**
     * Creates a {@code Semaphore} with the given number of
     * permits and nonfair fairness setting.
     *
     * @param permits the initial number of permits available.
     *        This value may be negative, in which case releases
     *        must occur before any acquires will be granted.
     * @param vertx the Vertx instance.
     */
    public Semaphore(int permits, io.vertx.rxjava.core.Vertx vertx) {
        this(permits, (Vertx) vertx.getDelegate());
    }

    /**
     * Creates a {@code Semaphore} with the given number of
     * permits and the given fairness setting.
     *
     * @param permits the initial number of permits available.
     *        This value may be negative, in which case releases
     *        must occur before any acquires will be granted.
     * @param fair {@code true} if this semaphore will guarantee
     *        first-in first-out granting of permits under contention,
     *        else {@code false}

     * @param vertx the Vertx instance.
     */
    public Semaphore(int permits, boolean fair, io.vertx.rxjava.core.Vertx vertx) {
        this(permits, fair, (Vertx) vertx.getDelegate());
    }

    /**
     * Acquires the given number of permits from this semaphore.
     *
     * <p>Acquires the given number of permits, if they are available,
     * and calls the action, reducing the number of available permits
     * by the given amount.
     *
     * <p>If insufficient permits are available then the request to acquire them
     * is enqueued until a call to release makes them available.
     * @param permits the number of permits to acquire
     * @param action the action to be called when permits are available.
     * @throws IllegalArgumentException if {@code permits} is negative
     */
    public void acquire(int permits, Runnable action){
        if (permits < 0) throw new IllegalArgumentException();
        synchronized (this){
            if (permits <= availablePermits){
                availablePermits -= permits;
                runOnContext(action);
            } else{
                deferredActions.offer(new DeferredContextAction(permits, action, vertx.getOrCreateContext()));
            }
        }
    }

    /**
     * Acquires one permit from this semaphore.
     *
     * <p>Acquires one permit, if it is available,
     * and calls the action, reducing the number of available permits
     * by one.
     *
     * <p>If insufficient permits are available then the request to acquire it
     * is enqueued until a call to release makes it available.
     * @param action the action to be called when permits are available.
     */
    public void acquire(Runnable action){
        acquire(1, action);
    }

    /**
     * Acquires the given number of permits from this semaphore, only if they are available at the
     * time of invocation.
     *
     * <p>Acquires a permit, if one is available and returns immediately,
     * with the value {@code true},
     * reducing the number of available permits by one.
     *
     * <p>If no permit is available then this method will return
     * immediately with the value {@code false}.
     *
     * <p>Even when this semaphore has been set to use a
     * fair ordering policy, a call to {@code tryAcquire(int)} <em>will</em>
     * immediately acquire a permit if one is available, whether or not
     * other attempts to acquire are currently waiting.
     * This &quot;barging&quot; behavior can be useful in certain
     * circumstances, even though it breaks fairness. If you want to honor
     * the fairness setting, then use
     * {@link #tryAcquire(int, long, TimeUnit, Consumer)}
     * which is almost equivalent.
     *
     * @param permits the number of permits to acquire
     * @return {@code true} if a permit was acquired and {@code false}
     *         otherwise
     */
    public synchronized boolean tryAcquire(int permits){
        if (permits < 0) throw new IllegalArgumentException();
        if (permits <= availablePermits){
            availablePermits -= permits;
            return true;
        } else{
            return false;
        }
    }

    /**
     * Acquires a permit from this semaphore, only if one is available at the
     * time of invocation.
     *
     * <p>Acquires one permit, if it is available and returns immediately,
     * with the value {@code true},
     * reducing the number of available permits by one.
     *
     * <p>If no permits are available then this method will return
     * immediately with the value {@code false}.
     *
     * <p>Even when this semaphore has been set to use a
     * fair ordering policy, a call to {@code tryAcquire()} <em>will</em>
     * immediately acquire a permit if one is available, whether or not
     * other attempts to acquire are currently waiting.
     * This &quot;barging&quot; behavior can be useful in certain
     * circumstances, even though it breaks fairness. If you want to honor
     * the fairness setting, then use
     * {@link #tryAcqure(long, TimeUnit, Consumer)}
     * which is almost equivalent.
     *
     * @return {@code true} if a permit was acquired and {@code false}
     *         otherwise
     */
    public  boolean tryAcquire(){
        return tryAcquire(1);
    }


    /**
     * Acquires the given number of permits from this semaphore, if all
     * become available within the given waiting time.
     *
     * <p>Acquires the given number of permits, if they are available, and
     * immediately passes {@code true} to {@code resultHandler},
     * reducing the number of available permits by the given amount.
     *
     * <p>If insufficient permits are available then
     * the request to acquire is enqueued until one of two things happens:
     * <ul>
     * <li>The application invokes one of the {@link #release() release}
     * methods for this semaphore, this request is next to be assigned
     * permits and the number of available permits satisfies this request; or
     * <li>The specified waiting time elapses.
     * </ul>
     *
     * <p>If the permits are acquired then the value {@code true} is passed to
     * {@code resultHandler}.
     *
     *
     * <p>If the specified waiting time elapses then the value {@code false}
     * is passed to {@code resultHandler}.
     *
     * @param permits the number of permits to acquire
     * @param timeout the maximum time to wait for the permits
     * @param unit the time unit of the {@code timeout} argument
     * @param resultHandler is passed {@code true} is the permits were acquired, otherwise
     * {@code false}
     * @throws IllegalArgumentException if {@code permits} is negative
     */
    public void tryAcquire(int permits, long timeout, TimeUnit unit, Consumer<Boolean> resultHandler){
        if (permits < 0) throw new IllegalArgumentException();
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

    /**
     * Acquires one permit from this semaphore, if it
     * becomes available within the given waiting time.
     *
     * <p>Acquires one permit, if it is available, and
     * immediately passes {@code true} to {@code resultHandler},
     * reducing the number of available permits by one.
     *
     * <p>If insufficient permits are available then
     * the request to acquire is enqueued until one of two things happens:
     * <ul>
     * <li>The application invokes one of the {@link #release() release}
     * methods for this semaphore, this request is next to be assigned
     * permits and the number of available permits satisfies this request; or
     * <li>The specified waiting time elapses.
     * </ul>
     *
     * <p>If the permit is acquired then the value {@code true} is passed to
     * {@code resultHandler}.
     *
     *
     * <p>If the specified waiting time elapses then the value {@code false}
     * is passed to {@code resultHandler}.
     *
     * @param timeout the maximum time to wait for the permits
     * @param unit the time unit of the {@code timeout} argument
     * @param resultHandler is passed {@code true} is the permits were acquired, otherwise
     * {@code false}
     * @throws IllegalArgumentException if {@code permits} is negative
     */
    public void tryAcqure(long timeout, TimeUnit unit, Consumer<Boolean> resultHandler){
        tryAcquire(1, timeout, unit, resultHandler);
    }

    /**
     * Releases the given number of permits, returning them to the semaphore.
     *
     * <p>Releases the given number of permits, increasing the number of
     * available permits by that amount.
     * <p>If any enqueued requests are trying to acquire permits, then one
     * is selected and given the permits that were just released.
     * If the number of available permits satisfies that request then its
     * {@code action} is invoked.
     * If there are still permits available
     * after this request has been satisfied, then those permits
     * are assigned in turn to other requests trying to acquire permits.
     *
     * @param permits the number of permits to release
     * @throws IllegalArgumentException if {@code permits} is negative
     */
    public synchronized void release(int permits){
        if (permits < 0) throw new IllegalArgumentException();
        availablePermits += permits;
        DeferredContextAction action = deferredActions.peek();
        while (action != null && action.getPermits() <= availablePermits){
            availablePermits -= action.getPermits();
            action.run();
            deferredActions.remove();
            action = deferredActions.peek();
        }
    }

    /**
     * Releases one permit, returning it to the semaphore.
     *
     * <p>Releases one permit, increasing the number of
     * available permits by one.
     * <p>If any enqueued requests are trying to acquire permits, then one
     * is selected and given the permit that was just released.
     * If the number of available permits satisfies that request then its
     * {@code action} is invoked.
     */
    public void release(){
        release(1);
    }

    /**
     * Acquires and returns all permits that are immediately available.
     *
     * @return the number of permits acquired
     */
    public synchronized int drainPermits(){
        int permits = availablePermits;
        availablePermits = 0;
        return permits;
    }

    /**
     * Returns the number of enqueued requests waiting to acquire.
     *
     * @return the number of enqueued requests waiting to acquire
     */
    public int getQueueLength(){
        return deferredActions.size();
    }

    /**
     * Returns the current number of permits available in this semaphore.
     *
     * <p>This method is typically used for debugging and testing purposes.
     *
     * @return the number of permits available in this semaphore
     */
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
