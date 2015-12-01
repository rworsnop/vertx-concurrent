# Vert.x Concurrent

A selection of utilities from java.util.concurrent, in Vert.x-aware, non-blocking form.

See the [api docs](http://rworsnop.github.io/vertx-concurrent/apidocs/)

## Getting the library 

Either grab the latest from the [releases page](https://github.com/rworsnop/vertx-beans/releases) or add a Maven dependency:
```
<dependency>
    <groupId>com.github.rworsnop</groupId>
    <artifactId>vertx-concurrent</artifactId>
    <version>1.0.0</version>
</dependency>
```


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

```
// Don't process more than 30,000 requests in a 10-second window
Semaphore semaphore = new Semaphore(30_000, vertx);
vertx.createHttpServer().requestHandler(req->{
    semaphore.acquire(()->req.response().end("Permits: " + semaphore.getAvailablePermits()));
    vertx.setTimer(10_000, id->semaphore.release());
}).listen(8082);

```