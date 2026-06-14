package rx.core;

import rx.disposable.Disposable;
import rx.schedulers.Scheduler;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

public class Observable<T> {

    @FunctionalInterface
    public interface OnSubscribe<T> {
        void subscribe(ObservableEmitter<T> emitter);
    }

    private final OnSubscribe<T> source;

    private Observable(OnSubscribe<T> source) {
        this.source = source;
    }

    public static <T> Observable<T> create(OnSubscribe<T> source) {
        return new Observable<>(source);
    }

    @SafeVarargs
    public static <T> Observable<T> just(T... items) {
        return create(emitter -> {
            for (T item : items) {
                if (emitter.isDisposed()) return;
                emitter.onNext(item);
            }
            emitter.onComplete();
        });
    }

    public static <T> Observable<T> empty() {
        return create(ObservableEmitter::onComplete);
    }

    public static <T> Observable<T> error(Throwable t) {
        return create(emitter -> emitter.onError(t));
    }

    public <R> Observable<R> map(Function<T, R> mapper) {
        Observable<T> upstream = this;

        return create(emitter -> {
            upstream.subscribe(new Observer<T>() {
                @Override
                public void onNext(T item) {
                    try {
                        R result = mapper.apply(item);
                        emitter.onNext(result);
                    } catch (Throwable t) {
                        emitter.onError(t);
                    }
                }

                @Override
                public void onError(Throwable t) {
                    emitter.onError(t); }

                @Override
                public void onComplete() {
                    emitter.onComplete();
                }
            });
        });
    }

    public Observable<T> filter(Predicate<T> predicate) {
        Observable<T> upstream = this;

        return create(emitter -> {
            upstream.subscribe(new Observer<T>() {
                @Override
                public void onNext(T item) {
                    try {
                        if (predicate.test(item)) {
                            emitter.onNext(item);
                        }
                    } catch (Throwable t) {
                        emitter.onError(t);
                    }
                }

                @Override
                public void onError(Throwable t) { emitter.onError(t); }

                @Override
                public void onComplete() { emitter.onComplete(); }
            });
        });
    }

    public <R> Observable<R> flatMap(Function<T, Observable<R>> mapper) {
        Observable<T> upstream = this;

        return create(emitter -> {
            upstream.subscribe(new Observer<T>() {
                @Override
                public void onNext(T item) {
                    if (emitter.isDisposed()) return;
                    try {
                        Observable<R> inner = mapper.apply(item);
                        inner.subscribe(new Observer<R>() {
                            @Override
                            public void onNext(R r) { emitter.onNext(r); }

                            @Override
                            public void onError(Throwable t) { emitter.onError(t); }

                            @Override
                            public void onComplete() {
                            }
                        });
                    } catch (Throwable t) {
                        emitter.onError(t);
                    }
                }

                @Override
                public void onError(Throwable t) { emitter.onError(t); }

                @Override
                public void onComplete() { emitter.onComplete(); }
            });
        });
    }

    public Observable<T> subscribeOn(Scheduler scheduler) {
        Observable<T> upstream = this;

        return create(emitter -> {
            scheduler.execute(() -> {
                upstream.subscribe(new Observer<T>() {
                    @Override
                    public void onNext(T item) { emitter.onNext(item); }
                    @Override
                    public void onError(Throwable t) { emitter.onError(t); }
                    @Override
                    public void onComplete() { emitter.onComplete(); }
                });
            });
        });
    }

    public Observable<T> observeOn(Scheduler scheduler) {
        Observable<T> upstream = this;

        return create(emitter -> {
            upstream.subscribe(new Observer<T>() {
                @Override
                public void onNext(T item) {
                    scheduler.execute(() -> emitter.onNext(item));
                }

                @Override
                public void onError(Throwable t) {
                    scheduler.execute(() -> emitter.onError(t));
                }

                @Override
                public void onComplete() {
                    scheduler.execute(emitter::onComplete);
                }
            });
        });
    }


    public Disposable subscribe(Observer<T> observer) {
        ObservableEmitter<T> emitter = new ObservableEmitter<>(observer);
        try {
            source.subscribe(emitter);
        } catch (Throwable t) {
            emitter.onError(t);
        }
        return emitter;
    }

    public Disposable subscribe(Consumer<T> onNext) {
        return subscribe(onNext, Throwable::printStackTrace, () -> {});
    }

    public Disposable subscribe(Consumer<T> onNext, Consumer<Throwable> onError) {
        return subscribe(onNext, onError, () -> {});
    }

    public Disposable subscribe(Consumer<T> onNext,
                                Consumer<Throwable> onError,
                                Runnable onComplete) {
        return subscribe(new Observer<T>() {
            @Override public void onNext(T item) { onNext.accept(item); }
            @Override public void onError(Throwable t) { onError.accept(t); }
            @Override public void onComplete() { onComplete.run(); }
        });
    }
}
