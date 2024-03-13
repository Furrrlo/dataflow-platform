package it.polimi.ds.dataflow.coordinator;

import it.polimi.ds.dataflow.coordinator.socket.CoordinatorSocketManager;
import it.polimi.ds.dataflow.coordinator.socket.CoordinatorSocketManagerImpl;
import it.polimi.ds.dataflow.coordinator.socket.PingPongHandler;
import it.polimi.ds.dataflow.socket.packets.HelloPacket;
import it.polimi.ds.dataflow.socket.packets.PingPacket;
import it.polimi.ds.dataflow.utils.Closeables;
import it.polimi.ds.dataflow.utils.SuppressFBWarnings;
import org.jetbrains.annotations.Unmodifiable;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.ServerSocket;
import java.net.SocketException;
import java.nio.channels.ClosedByInterruptException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

public final class WorkerManager implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(WorkerManager.class);

    private final ServerSocket socket;
    private final ExecutorService threadPool;
    private final @Nullable AutoCloseable onUnrecoverableException;

    private final Supplier<@Nullable Future<?>> acceptLoopTask;
    private final Collection<Future<?>> workersTasks = ConcurrentHashMap.newKeySet();

    private final Set<WorkerClient> workers = ConcurrentHashMap.newKeySet();
    private final ConcurrentMap<ReconnectListenerKey, Set<Runnable>> reconnectListeners = new ConcurrentHashMap<>();
    private final Set<CompletableFuture<WorkerClient>> reconnectionFutures = ConcurrentHashMap.newKeySet();

    public static WorkerManager listen(ExecutorService threadPool,
                                       int port,
                                       @Nullable AutoCloseable onUnrecoverableException) throws IOException {
        final var taskRef = new AtomicReference<@Nullable Future<?>>();
        WorkerManager mngr = new WorkerManager(
                threadPool,
                new ServerSocket(port),
                onUnrecoverableException,
                taskRef::get);
        taskRef.set(threadPool.submit(mngr::execute));
        return mngr;
    }

    private WorkerManager(ExecutorService threadPool,
                          ServerSocket socket,
                          @Nullable AutoCloseable onUnrecoverableException,
                          Supplier<Future<?>> acceptLoopTask) {
        this.threadPool = threadPool;
        this.socket = socket;
        this.onUnrecoverableException = onUnrecoverableException;
        this.acceptLoopTask = acceptLoopTask;
    }

    private void execute() {
        try {
            while (!Thread.interrupted()) {
                var workerSocket = socket.accept();
                if(Thread.interrupted()) {
                    workerSocket.close();
                    Thread.currentThread().interrupt();
                    break;
                }

                CoordinatorSocketManager worker = new CoordinatorSocketManagerImpl(threadPool, workerSocket);
                workerSocket.setSoTimeout(PingPacket.TIMEOUT_MILLIS);
                workersTasks.add(threadPool.submit(new PingPongHandler(worker)));

                var helloTaskRef = new AtomicReference<Future<?>>();
                var task = threadPool.submit(() -> {
                    try {
                        onNewConnection(worker);
                    } catch (Throwable ex) {
                        LOGGER.error("Uncaught exception in #onNewConnection(worker) call", ex);
                    } finally {
                        var helloTask = helloTaskRef.get();
                        if(helloTask != null)
                            workersTasks.remove(helloTask);
                    }
                });
                helloTaskRef.set(task);
                workersTasks.add(task);
            }
        } catch (ClosedByInterruptException ex) {
            Thread.currentThread().interrupt();
        } catch (Throwable ex) {
            @SuppressWarnings("PMD.AvoidInstanceofChecksInCatchClause")
            boolean isSocketException = ex instanceof SocketException;
            // Virtual thread was interrupted during accept
            if(isSocketException && Thread.currentThread().isInterrupted())
                return;

            LOGGER.error("Uncaught exception in accept loop", ex);

            try {
                if(onUnrecoverableException != null)
                    onUnrecoverableException.close();
            } catch (Throwable t) {
                var th = Thread.currentThread();
                th.getUncaughtExceptionHandler().uncaughtException(th, t);
            }
        }
    }

    private void onNewConnection(CoordinatorSocketManager worker) throws IOException {
        try (var ctx = worker.receive(HelloPacket.class, 10, TimeUnit.SECONDS)) {
            var helloPkt = ctx.getPacket();

            var workerClient = new WorkerClient(worker, helloPkt.uuid(), helloPkt.dfsNodeName());
            worker.setOnClose(cls -> {
                try {
                    cls.close();
                } finally {
                    workers.remove(workerClient);
                }
            });

            var reconnectionFutures = new HashSet<>(this.reconnectionFutures);
            reconnectionFutures.forEach(f -> f.complete(workerClient));
            this.reconnectionFutures.removeAll(reconnectionFutures);

            helloPkt.previousJobs().forEach(job -> {
                var reconnectListeners = this.reconnectListeners.remove(new ReconnectListenerKey(
                        helloPkt.uuid(), job.jobId(), job.partition()));
                reconnectListeners.forEach(Runnable::run);
            });

            workers.add(workerClient);
        } catch (InterruptedIOException ex) {
            worker.close();
            Thread.currentThread().interrupt();
        } catch (TimeoutException e) {
            throw new IOException("Worker failed to send hello packet", e);
        }
    }

    public void registerReconnectConsumerFor(UUID worker, int jobId, int partition, Runnable runnable) {
        reconnectListeners.computeIfAbsent(
                new ReconnectListenerKey(worker, jobId, partition),
                _ -> ConcurrentHashMap.newKeySet()
        ).add(runnable);
    }

    public Future<WorkerClient> waitForAnyReconnections() {
        var future = new CompletableFuture<WorkerClient>();
        reconnectionFutures.add(future);
        return future;
    }

    public void unregisterConsumersForJob(int jobId) {
        reconnectListeners.keySet().removeIf(k -> k.jobId() == jobId);
    }

    public void unregisterConsumersForJob(int jobId, int partition) {
        reconnectListeners.keySet().removeIf(k -> k.jobId() == jobId && k.partition() == partition);
    }

    @Override
    public void close() throws IOException {
        var acceptLoopTask = this.acceptLoopTask.get();
        if(acceptLoopTask != null)
            acceptLoopTask.cancel(true);

        for (var t : workersTasks) {
            t.cancel(true);
        }

        Closeables.closeAll(Stream.concat(
                        Stream.of(socket),
                        workers.stream().map(WorkerClient::getSocket)),
                e -> new IOException("Failed to close WorkerManager", e));
    }

    public @Unmodifiable Set<WorkerClient> getWorkers() {
        return workers;
    }

    @SuppressFBWarnings({"IMPROPER_UNICODE"}) // Not security sensitive
    public @Unmodifiable List<WorkerClient> getCloseToDfsNode(String dfsNode) {
        // This is weird, but it's to avoid having spotbugs
        // report the issue on the lambda where I cannot suppress it
        Predicate<String> equalsIgnoreCase = dfsNode::equalsIgnoreCase;
        return workers.stream()
                .filter(w -> equalsIgnoreCase.test(w.getDfsNodeName()))
                .toList();
    }

    private record ReconnectListenerKey(UUID uuid, int jobId, int partition) {
    }
}
