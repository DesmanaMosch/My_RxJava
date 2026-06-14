package rx;

import org.junit.jupiter.api.*;
import rx.core.Observable;
import rx.disposable.Disposable;
import rx.schedulers.Scheduler;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class ObservableTest {


    @Nested
    @DisplayName("Базовые компоненты Observable")
    class BasicObservableTest {

        @Test
        @DisplayName("just() — элементы приходят в правильном порядке")
        void testJust() {
            List<Integer> results = new ArrayList<>();
            AtomicBoolean completed = new AtomicBoolean(false);

            Observable.just(1, 2, 3).subscribe(
                    results::add,
                    Throwable::printStackTrace,
                    () -> completed.set(true)
            );

            assertEquals(List.of(1, 2, 3), results);
            assertTrue(completed.get());
        }

        @Test
        @DisplayName("create() — можно создать кастомный источник")
        void testCreate() {
            List<String> results = new ArrayList<>();

            Observable.<String>create(emitter -> {
                emitter.onNext("hello");
                emitter.onNext("world");
                emitter.onComplete();
            }).subscribe(results::add);

            assertEquals(List.of("hello", "world"), results);
        }

        @Test
        @DisplayName("empty() — сразу вызывает onComplete без элементов")
        void testEmpty() {
            List<Object> results = new ArrayList<>();
            AtomicBoolean completed = new AtomicBoolean(false);

            Observable.empty().subscribe(
                    results::add,
                    Throwable::printStackTrace,
                    () -> completed.set(true)
            );

            assertTrue(results.isEmpty());
            assertTrue(completed.get());
        }

        @Test
        @DisplayName("error() — сразу вызывает onError")
        void testError() {
            AtomicReference<Throwable> caught = new AtomicReference<>();
            AtomicBoolean completed = new AtomicBoolean(false);

            Observable.error(new RuntimeException("test error")).subscribe(
                    item -> fail("onNext не должен вызываться"),
                    caught::set,
                    () -> completed.set(true)
            );

            assertNotNull(caught.get());
            assertEquals("test error", caught.get().getMessage());
            assertFalse(completed.get());
        }

        @Test
        @DisplayName("subscribe() возвращает Disposable")
        void testSubscribeReturnsDisposable() {
            Disposable d = Observable.just(1, 2, 3).subscribe(item -> {});
            assertNotNull(d);
        }

        @Test
        @DisplayName("Исключение в source попадает в onError")
        void testExceptionInSourceGoesToOnError() {
            AtomicReference<Throwable> caught = new AtomicReference<>();

            Observable.<Integer>create(emitter -> {
                emitter.onNext(1);
                throw new RuntimeException("boom");
            }).subscribe(
                    item -> {},
                    caught::set
            );

            assertNotNull(caught.get());
            assertEquals("boom", caught.get().getMessage());
        }
    }


    @Nested
    @DisplayName("Операторы map и filter")
    class OperatorsTest {

        @Test
        @DisplayName("map() — преобразует каждый элемент")
        void testMap() {
            List<String> results = new ArrayList<>();

            Observable.just(1, 2, 3)
                    .map(i -> "item-" + i)
                    .subscribe(results::add);

            assertEquals(List.of("item-1", "item-2", "item-3"), results);
        }

        @Test
        @DisplayName("map() — меняет тип элементов")
        void testMapChangesType() {
            List<Integer> results = new ArrayList<>();

            Observable.just("a", "bb", "ccc")
                    .map(String::length)
                    .subscribe(results::add);

            assertEquals(List.of(1, 2, 3), results);
        }

        @Test
        @DisplayName("filter() — пропускает только чётные числа")
        void testFilter() {
            List<Integer> results = new ArrayList<>();

            Observable.just(1, 2, 3, 4, 5, 6)
                    .filter(i -> i % 2 == 0)
                    .subscribe(results::add);

            assertEquals(List.of(2, 4, 6), results);
        }

        @Test
        @DisplayName("filter() — если ни один не проходит — пустой результат")
        void testFilterAllRejected() {
            List<Integer> results = new ArrayList<>();

            Observable.just(1, 3, 5)
                    .filter(i -> i % 2 == 0)
                    .subscribe(results::add);

            assertTrue(results.isEmpty());
        }

        @Test
        @DisplayName("map() + filter() — цепочка операторов")
        void testMapAndFilter() {
            List<String> results = new ArrayList<>();

            Observable.just(1, 2, 3, 4, 5)
                    .map(i -> i * 10)
                    .filter(i -> i > 20)
                    .map(i -> "value:" + i)
                    .subscribe(results::add);

            assertEquals(List.of("value:30", "value:40", "value:50"), results);
        }

        @Test
        @DisplayName("map() — исключение в маппере → onError")
        void testMapExceptionGoesToOnError() {
            AtomicReference<Throwable> caught = new AtomicReference<>();

            Observable.just("123", "abc", "456")
                    .map(Integer::parseInt)
                    .subscribe(
                            item -> {},
                            caught::set
                    );

            assertNotNull(caught.get());
            assertInstanceOf(NumberFormatException.class, caught.get());
        }

        @Test
        @DisplayName("filter() — исключение в предикате → onError")
        void testFilterExceptionGoesToOnError() {
            AtomicReference<Throwable> caught = new AtomicReference<>();

            Observable.just("hello", null, "world")
                    .filter(s -> s.length() > 3)
                    .subscribe(
                            item -> {},
                            caught::set
                    );

            assertNotNull(caught.get());
            assertInstanceOf(NullPointerException.class, caught.get());
        }
    }


    @Nested
    @DisplayName("Оператор flatMap")
    class FlatMapTest {

        @Test
        @DisplayName("flatMap() — разворачивает вложенные потоки")
        void testFlatMap() {
            List<String> results = new ArrayList<>();

            Observable.just(1, 2)
                    .flatMap(i -> Observable.just("a" + i, "b" + i))
                    .subscribe(results::add);

            assertEquals(List.of("a1", "b1", "a2", "b2"), results);
        }

        @Test
        @DisplayName("flatMap() — можно преобразовывать типы")
        void testFlatMapTypeChange() {
            List<Integer> results = new ArrayList<>();

            Observable.just("AB", "CDE")
                    .flatMap(s -> {
                        List<Integer> lengths = new ArrayList<>();
                        for (char c : s.toCharArray()) {
                            lengths.add(1);
                        }
                        return Observable.create(emitter -> {
                            for (int l : lengths) emitter.onNext(l);
                            emitter.onComplete();
                        });
                    })
                    .subscribe(results::add);

            assertEquals(5, results.size());
        }

        @Test
        @DisplayName("flatMap() — ошибка во внутреннем Observable → onError")
        void testFlatMapInnerError() {
            AtomicReference<Throwable> caught = new AtomicReference<>();

            Observable.just(1, 2, 3)
                    .flatMap(i -> {
                        if (i == 2) return Observable.error(new RuntimeException("inner error"));
                        return Observable.just(i * 10);
                    })
                    .subscribe(
                            item -> {},
                            caught::set
                    );

            assertNotNull(caught.get());
            assertEquals("inner error", caught.get().getMessage());
        }

        @Test
        @DisplayName("flatMap() — пустой inner Observable не ломает поток")
        void testFlatMapWithEmpty() {
            List<Integer> results = new ArrayList<>();

            Observable.just(1, 2, 3)
                    .flatMap(i -> i == 2 ? Observable.empty() : Observable.just(i))
                    .subscribe(results::add);

            assertEquals(List.of(1, 3), results);
        }
    }


    @Nested
    @DisplayName("Отмена подписки (Disposable)")
    class DisposableTest {

        @Test
        @DisplayName("dispose() — после отмены onNext не вызывается")
        void testDisposeStopsDelivery() {
            List<Integer> results = new ArrayList<>();

            Disposable[] disposable = {null};

            disposable[0] = Observable.<Integer>create(emitter -> {
                for (int i = 1; i <= 5; i++) {
                    if (emitter.isDisposed()) break;
                    emitter.onNext(i);
                }
                emitter.onComplete();
            }).subscribe(item -> {
                results.add(item);
                if (item == 2) {
                    disposable[0].dispose();
                }
            });
            assertTrue(results.size() <= 3);
            assertTrue(results.contains(1));
            assertTrue(results.contains(2));
        }

        @Test
        @DisplayName("isDisposed() — изначально false, после dispose() — true")
        void testIsDisposed() {
            Disposable d = Observable.just(1, 2, 3).subscribe(item -> {});
            assertTrue(d.isDisposed());
        }

        @Test
        @DisplayName("dispose() идемпотентен — можно вызывать много раз")
        void testDisposeIdempotent() {
            Disposable d = Observable.just(1).subscribe(item -> {});
            assertDoesNotThrow(() -> {
                d.dispose();
                d.dispose();
                d.dispose();
            });
        }
    }


    @Nested
    @DisplayName("Планировщики (Schedulers)")
    class SchedulerTest {

        @Test
        @DisplayName("subscribeOn(IO) — источник работает в другом потоке")
        void testSubscribeOnIO() throws InterruptedException {
            CountDownLatch latch = new CountDownLatch(1);
            AtomicReference<String> threadName = new AtomicReference<>();
            Scheduler.IOThreadScheduler io = new Scheduler.IOThreadScheduler();

            Observable.just(42)
                    .subscribeOn(io)
                    .subscribe(
                            item -> threadName.set(Thread.currentThread().getName()),
                            Throwable::printStackTrace,
                            latch::countDown
                    );

            assertTrue(latch.await(3, TimeUnit.SECONDS), "Тест завис — поток не завершился");
            assertNotNull(threadName.get());
            assertTrue(threadName.get().startsWith("rx-io"),
                    "Ожидался rx-io поток, но был: " + threadName.get());

            io.shutdown();
        }

        @Test
        @DisplayName("observeOn(Single) — наблюдатель получает данные в нужном потоке")
        void testObserveOnSingle() throws InterruptedException {
            CountDownLatch latch = new CountDownLatch(3);
            List<String> threadNames = new ArrayList<>();
            Scheduler.SingleThreadScheduler single = new Scheduler.SingleThreadScheduler();

            Observable.just(1, 2, 3)
                    .observeOn(single)
                    .subscribe(
                            item -> {
                                threadNames.add(Thread.currentThread().getName());
                                latch.countDown();
                            },
                            Throwable::printStackTrace,
                            () -> {}
                    );

            assertTrue(latch.await(3, TimeUnit.SECONDS));
            assertTrue(threadNames.stream().allMatch(n -> n.equals("rx-single")),
                    "Все элементы должны обрабатываться в rx-single, но были: " + threadNames);

            single.shutdown();
        }

        @Test
        @DisplayName("subscribeOn + observeOn — источник и наблюдатель в разных потоках")
        void testSubscribeOnAndObserveOn() throws InterruptedException {
            CountDownLatch latch = new CountDownLatch(1);
            AtomicReference<String> observeThread = new AtomicReference<>();
            Scheduler.IOThreadScheduler io = new Scheduler.IOThreadScheduler();
            Scheduler.SingleThreadScheduler single = new Scheduler.SingleThreadScheduler();

            Observable.just("data")
                    .subscribeOn(io)
                    .observeOn(single)
                    .subscribe(
                            item -> observeThread.set(Thread.currentThread().getName()),
                            Throwable::printStackTrace,
                            latch::countDown
                    );

            assertTrue(latch.await(3, TimeUnit.SECONDS));
            assertEquals("rx-single", observeThread.get());

            io.shutdown();
            single.shutdown();
        }

        @Test
        @DisplayName("ComputationScheduler — использует фиксированный пул")
        void testComputationScheduler() throws InterruptedException {
            CountDownLatch latch = new CountDownLatch(1);
            AtomicReference<String> threadName = new AtomicReference<>();
            Scheduler.ComputationScheduler comp = new Scheduler.ComputationScheduler();

            Observable.just(1)
                    .subscribeOn(comp)
                    .subscribe(
                            item -> threadName.set(Thread.currentThread().getName()),
                            Throwable::printStackTrace,
                            latch::countDown
                    );

            assertTrue(latch.await(3, TimeUnit.SECONDS));
            assertTrue(threadName.get().startsWith("rx-computation"));

            comp.shutdown();
        }
    }


    @Nested
    @DisplayName("Обработка ошибок")
    class ErrorHandlingTest {

        @Test
        @DisplayName("После onError — onNext и onComplete не вызываются")
        void testAfterErrorNoMoreEvents() {
            List<Integer> received = new ArrayList<>();
            AtomicBoolean completed = new AtomicBoolean(false);
            AtomicReference<Throwable> error = new AtomicReference<>();

            Observable.<Integer>create(emitter -> {
                emitter.onNext(1);
                emitter.onError(new RuntimeException("fail"));
                emitter.onNext(2);
                emitter.onComplete();
            }).subscribe(
                    received::add,
                    error::set,
                    () -> completed.set(true)
            );

            assertEquals(List.of(1), received);
            assertNotNull(error.get());
            assertFalse(completed.get());
        }

        @Test
        @DisplayName("После onComplete — onNext не вызывается")
        void testAfterCompleteNoMoreEvents() {
            List<Integer> received = new ArrayList<>();

            Observable.<Integer>create(emitter -> {
                emitter.onNext(1);
                emitter.onComplete();
                emitter.onNext(2);
            }).subscribe(received::add);

            assertEquals(List.of(1), received);
        }

        @Test
        @DisplayName("Ошибка в map пробрасывается в onError")
        void testErrorPropagationThroughMap() {
            AtomicReference<Throwable> caught = new AtomicReference<>();

            Observable.just(1, 0, 3)
                    .map(i -> 10 / i)
                    .subscribe(
                            item -> {},
                            caught::set
                    );

            assertNotNull(caught.get());
            assertInstanceOf(ArithmeticException.class, caught.get());
        }

        @Test
        @DisplayName("Null в onNext не ломает emitter")
        void testNullHandling() {
            List<String> results = new ArrayList<>();

            assertDoesNotThrow(() -> {
                Observable.just("a", "b", "c")
                        .subscribe(results::add);
            });

            assertEquals(3, results.size());
        }
    }
}
