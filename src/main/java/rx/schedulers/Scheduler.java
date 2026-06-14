package rx.schedulers;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public interface Scheduler {
    void execute(Runnable task);

    class IOThreadScheduler implements Scheduler {
        private final ExecutorService executor = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "rx-io-" + System.nanoTime() % 10000);
            t.setDaemon(true);
            return t;
        });

        @Override
        public void execute(Runnable task) {
            executor.submit(task);
        }

        public void shutdown() { executor.shutdown(); }
    }

    class ComputationScheduler implements Scheduler {
        private final ExecutorService executor = Executors.newFixedThreadPool(
                Runtime.getRuntime().availableProcessors(),
                r -> {
                    Thread t = new Thread(r, "rx-computation-" + System.nanoTime() % 10000);
                    t.setDaemon(true);
                    return t;
                }
        );

        @Override
        public void execute(Runnable task) {
            executor.submit(task);
        }

        public void shutdown() { executor.shutdown(); }
    }

    class SingleThreadScheduler implements Scheduler {
        private final ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "rx-single");
            t.setDaemon(true);
            return t;
        });

        @Override
        public void execute(Runnable task) {
            executor.submit(task);
        }

        public void shutdown() { executor.shutdown(); }
    }
}
