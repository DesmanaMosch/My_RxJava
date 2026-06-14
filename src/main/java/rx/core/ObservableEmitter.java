package rx.core;

import rx.disposable.BooleanDisposable;
import rx.disposable.Disposable;

public class ObservableEmitter<T> implements Disposable {

    private final Observer<T> downstream;
    private final BooleanDisposable disposable = new BooleanDisposable();
    private volatile boolean terminated = false;

    public ObservableEmitter(Observer<T> downstream) {
        this.downstream = downstream;
    }

    public void onNext(T item) {
        if (terminated || isDisposed()) return;
        try {
            downstream.onNext(item);
        } catch (Throwable t) {
            onError(t);
        }
    }

    public void onError(Throwable t) {
        if (terminated || isDisposed()) return;
        terminated = true;
        try {
            downstream.onError(t);
        } finally {
            dispose();
        }
    }

    public void onComplete() {
        if (terminated || isDisposed()) return;
        terminated = true;
        try {
            downstream.onComplete();
        } finally {
            dispose();
        }
    }

    @Override
    public void dispose() {
        disposable.dispose();
    }

    @Override
    public boolean isDisposed() {
        return disposable.isDisposed();
    }
}
