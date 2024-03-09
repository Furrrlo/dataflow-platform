package it.polimi.ds.dataflow.coordinator;

import it.polimi.ds.dataflow.coordinator.socket.CoordinatorSocketManager;
import it.polimi.ds.dataflow.coordinator.socket.CoordinatorSocketManagerImpl;
import it.polimi.ds.dataflow.socket.packets.HelloPacket;
import it.polimi.ds.dataflow.utils.Closeables;
import it.polimi.ds.dataflow.utils.SuppressFBWarnings;
import org.jetbrains.annotations.Unmodifiable;

import java.io.Closeable;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.function.Predicate;
import java.util.stream.Stream;

public final class WorkerManager implements Closeable {

    private final ServerSocket socket;
    private final ExecutorService threadPool;

    private final Set<WorkerClient> workers = ConcurrentHashMap.newKeySet();
    private final ConcurrentMap<ReconnectListenerKey, Set<Runnable>> reconnectListeners = new ConcurrentHashMap<>();
    private final Set<CompletableFuture<WorkerClient>> reconnectionFutures = ConcurrentHashMap.newKeySet();

    public static WorkerManager listen(ExecutorService threadPool, int port) throws IOException {
        WorkerManager mngr = new WorkerManager(threadPool, new ServerSocket(port));
        threadPool.execute(mngr::execute);
        return mngr;
    }

    private WorkerManager(ExecutorService threadPool, ServerSocket socket) {
        this.threadPool = threadPool;
        this.socket = socket;
    }

    private void execute() {
        try {
            while (!Thread.interrupted()) {
                CoordinatorSocketManager worker = new CoordinatorSocketManagerImpl(threadPool, socket.accept());
                try (var ctx = worker.receive(HelloPacket.class)) {
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
                }
            }
        } catch (IOException e) {
            // TODO: all should die in a sea of flames
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
