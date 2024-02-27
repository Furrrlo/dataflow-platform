package it.polimi.ds.dataflow.coordinator;

import it.polimi.ds.dataflow.coordinator.socket.CoordinatorSocketManager;
import it.polimi.ds.dataflow.utils.ExceptionlessAutoCloseable;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public final class Worker {

    private final CoordinatorSocketManager socket;
    private final UUID uuid;
    private final String dfsNodeName;

    private final AtomicInteger currentScheduledJobs = new AtomicInteger();

    public Worker(CoordinatorSocketManager socket, UUID uuid, String dfsNodeName) {
        this.socket = socket;
        this.uuid = uuid;
        this.dfsNodeName = dfsNodeName;
    }

    public CoordinatorSocketManager getSocket() {
        return socket;
    }

    public UUID getUuid() {
        return uuid;
    }

    public String getDfsNodeName() {
        return dfsNodeName;
    }

    public ExceptionlessAutoCloseable scheduleJob() {
        currentScheduledJobs.incrementAndGet();
        return currentScheduledJobs::decrementAndGet;
    }

    public int getCurrentScheduledJobs() {
        return currentScheduledJobs.get();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (Worker) obj;
        return Objects.equals(this.socket, that.socket) &&
                Objects.equals(this.dfsNodeName, that.dfsNodeName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(socket, dfsNodeName);
    }

    @Override
    public String toString() {
        return "Worker{" +
                "socket=" + socket +
                ", dfsNodeName='" + dfsNodeName + '\'' +
                ", currentScheduledJobs='" + currentScheduledJobs.get() + '\'' +
                '}';
    }
}
