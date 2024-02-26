package it.polimi.ds.dataflow.coordinator;

import it.polimi.ds.dataflow.coordinator.socket.CoordinatorSocketManager;
import it.polimi.ds.dataflow.coordinator.socket.CoordinatorSocketManagerImpl;
import it.polimi.ds.dataflow.socket.packets.HelloPacket;
import it.polimi.ds.dataflow.utils.SuppressFBWarnings;
import org.jetbrains.annotations.Unmodifiable;

import java.io.Closeable;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Predicate;

public final class WorkerManager implements Closeable {

    private final ServerSocket socket;
    private final ExecutorService threadPool;

    private final Set<Worker> workers = ConcurrentHashMap.newKeySet();

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
            while(!Thread.interrupted()) {
                CoordinatorSocketManager worker = new CoordinatorSocketManagerImpl(threadPool, socket.accept());
                worker.receive(HelloPacket.class); // TODO: need to know to which dfs node its connected
                workers.add(new Worker(worker, ""));
            }
        } catch (IOException e) {
            // TODO: all should die in a sea of flames
        }
    }

    @Override
    public void close() throws IOException {
        final List<IOException> exs = new ArrayList<>();

        try {
            socket.close();
        } catch (IOException ex) {
            exs.add(ex);
        }

        for (Worker w : workers) {
            try {
                w.getSocket().close();
            } catch (IOException ex) {
                exs.add(ex);
            }
        }

        switch (exs.size()) {
            case 0 -> {}
            case 1 -> throw new IOException("Failed to close WorkerManager", exs.getFirst());
            default -> {
                IOException ex = new IOException("Failed to close WorkerManager");
                exs.forEach(ex::addSuppressed);
                throw ex;
            }
        }
    }

    public @Unmodifiable Set<Worker> getWorkers() {
        return workers;
    }

    @SuppressFBWarnings({"IMPROPER_UNICODE"}) // Not security sensitive
    public @Unmodifiable List<Worker> getCloseToDfsNode(String dfsNode) {
        // This is weird, but it's to avoid having spotbugs
        // report the issue on the lambda where I cannot suppress it
        Predicate<String> equalsIgnoreCase = dfsNode::equalsIgnoreCase;
        return workers.stream()
                .filter(w -> equalsIgnoreCase.test(w.getDfsNodeName()))
                .toList();
    }
}
