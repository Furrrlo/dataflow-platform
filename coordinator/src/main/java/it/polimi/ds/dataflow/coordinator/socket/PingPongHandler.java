package it.polimi.ds.dataflow.coordinator.socket;

import it.polimi.ds.dataflow.socket.packets.PingPacket;
import it.polimi.ds.dataflow.socket.packets.PongPacket;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.UncheckedIOException;

public class PingPongHandler implements Runnable {
    private final CoordinatorSocketManager worker;

    public PingPongHandler(CoordinatorSocketManager worker) {
        this.worker = worker;
    }

    @Override
    @SuppressWarnings("BusyWait")
    public void run() {
        try {
            while (!Thread.interrupted()) {
                PingPacket pingPacket = new PingPacket();
                try (var send = worker.send(pingPacket, PongPacket.class)) {
                    send.ack();
                }
                Thread.sleep(PingPacket.TIMEOUT_MILLIS / 2);
            }
        } catch (InterruptedException | InterruptedIOException e) {
            // STOPPED
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
