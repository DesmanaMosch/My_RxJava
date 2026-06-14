package demo;

import rx.core.Observable;
import rx.disposable.Disposable;
import rx.schedulers.Scheduler;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class Main {

    public static void main(String[] args) throws InterruptedException {

        demo1_basicOperators();
        demo2_errorHandling();
        demo3_flatMap();
        demo4_disposable();
        demo5_schedulers();
    }


    static void demo1_basicOperators() {
        System.out.println("\n=== ДЕМО 1: map + filter ===");

        Observable.just(1, 2, 3, 4, 5)
                .filter(i -> i % 2 == 0)
                .map(i -> "число: " + i)
                .subscribe(
                        item -> System.out.println("[onNext] " + item),
                        err  -> System.out.println("[onError] " + err.getMessage()),
                        ()   -> System.out.println("[onComplete]")
                );
    }

    static void demo2_errorHandling() {
        System.out.println("\n=== ДЕМО 2: обработка ошибок ===");

        Observable.just("10", "abc", "30")
                .map(Integer::parseInt)
                .subscribe(
                        item -> System.out.println("[onNext] " + item),
                        err  -> System.out.println("[onError] поймали: " + err.getClass().getSimpleName()),
                        ()   -> System.out.println("[onComplete]")
                );
    }

    static void demo3_flatMap() {
        System.out.println("\n=== ДЕМО 3: flatMap ===");

        Observable.just(1, 2, 3)
                .flatMap(i -> Observable.just(i * 10, i * 100))
                .subscribe(
                        item -> System.out.println("[onNext] " + item),
                        err  -> System.out.println("[onError] " + err.getMessage()),
                        ()   -> System.out.println("[onComplete]")
                );
    }

    static void demo4_disposable() {
        System.out.println("\n=== ДЕМО 4: Disposable ===");

        Disposable[] d = {null};

        d[0] = Observable.just(1, 2, 3, 4, 5)
                .subscribe(item -> {
                    System.out.println("[onNext] " + item);
                    if (item == 3 && d[0] != null) {
                        d[0].dispose();
                        System.out.println("[main] disposed после 3");
                    }
                });

        System.out.println("[main] isDisposed: " + d[0].isDisposed());
    }

    static void demo5_schedulers() throws InterruptedException {
        System.out.println("\n=== ДЕМО 5: Schedulers ===");

        Scheduler.IOThreadScheduler io = new Scheduler.IOThreadScheduler();
        Scheduler.SingleThreadScheduler single = new Scheduler.SingleThreadScheduler();

        CountDownLatch latch = new CountDownLatch(3);

        Observable.just(1, 2, 3)
                .subscribeOn(io)
                .observeOn(single)
                .subscribe(
                        item -> {
                            System.out.printf("[onNext] %d в потоке '%s'%n",
                                    item, Thread.currentThread().getName());
                            latch.countDown();
                        },
                        err -> System.out.println("[onError] " + err.getMessage()),
                        ()  -> System.out.println("[onComplete]")
                );

        latch.await(3, TimeUnit.SECONDS);
        io.shutdown();
        single.shutdown();
        System.out.println("[main] готово");
    }
}
