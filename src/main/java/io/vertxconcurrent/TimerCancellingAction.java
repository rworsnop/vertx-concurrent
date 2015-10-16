package io.vertxconcurrent;

import io.vertx.core.Vertx;

import java.util.Objects;

/**
 * Created by Rob Worsnop on 10/15/15.
 */
class TimerCancellingAction implements Runnable{
    final Vertx vertx;
    final Runnable delegate;
    Long timerId;

    public TimerCancellingAction(Vertx vertx, Runnable delegate) {
        this.vertx = vertx;
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