package rx.disposable;

public interface Disposable {
    void dispose();
    boolean isDisposed();
}
