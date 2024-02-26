package it.polimi.ds.dataflow.worker.socket;

import it.polimi.ds.dataflow.socket.SocketManagerImpl;
import it.polimi.ds.dataflow.socket.packets.C2SAckPacket;
import it.polimi.ds.dataflow.socket.packets.C2SPacket;
import it.polimi.ds.dataflow.socket.packets.S2CAckPacket;
import it.polimi.ds.dataflow.socket.packets.S2CPacket;

import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class WorkerSocketManagerImpl
        extends SocketManagerImpl<S2CPacket, S2CAckPacket, C2SAckPacket, C2SPacket>
        implements WorkerSocketManager {

    private final ExecutorService executor;

    public WorkerSocketManagerImpl(Socket socket) throws IOException {
        this(createDefaultExecutor(), socket);
    }

    private WorkerSocketManagerImpl(ExecutorService executor, Socket socket) throws IOException {
        super("Worker", executor, socket);
        this.executor = executor;
    }

    private static ExecutorService createDefaultExecutor() {
        return Executors.newFixedThreadPool(2, new ThreadFactory() {

            private final AtomicInteger threadNum = new AtomicInteger();

            @Override
            public Thread newThread(Runnable r) {
                var th = new Thread(r);
                th.setName("WorkerSocketManagerImpl-thread-" + threadNum.getAndIncrement());
                return th;
            }
        });
    }

    @Override
    protected void doClose() throws IOException {
        super.doClose();

        if (!executor.isShutdown())
            executor.shutdown();
    }
}
