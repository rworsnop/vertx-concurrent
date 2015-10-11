# Vert.x Concurrent

A selection of utilities from java.util.concurrent, in Vert.x-aware, non-blocking form.

See the [api docs](http://rworsnop.github.io/vertx-concurrent/apidocs/)

## Examples

### Semaphore

```
Semaphore semaphore = new Semaphore(50, vertx);
semaphore.acquire(10, ()->{
   // do some work
   semaphore.release(10);
}));
```

```
Semaphore semaphore = new Semaphore(50, vertx);
if (semaphore.tryAcquire(10){
    // do some work
    semaphore.release(10);
}
```

```
Semaphore semaphore = new Semaphore(50, vertx);
semaphore.tryAcquire(10, 30, SECONDS, success->{
    if (success){
        // do some work
        semaphore.release(10);
    } else {
        // timed out before we could acquire permits
    }
});
```