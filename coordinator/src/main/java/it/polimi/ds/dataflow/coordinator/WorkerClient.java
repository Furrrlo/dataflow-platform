package it.polimi.ds.dataflow.coordinator;

import it.polimi.ds.dataflow.coordinator.socket.CoordinatorSocketManager;
import it.polimi.ds.dataflow.utils.ExceptionlessAutoCloseable;

import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public final class WorkerClient {

    private final CoordinatorSocketManager socket;
    private final UUID uuid;
    private final String dfsNodeName;

    private final Set<ScheduledJob> currentScheduledJobs = ConcurrentHashMap.newKeySet();

    public WorkerClient(CoordinatorSocketManager socket, UUID uuid, String dfsNodeName) {
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

    public ExceptionlessAutoCloseable scheduleJob(int jobId, int partition) {
        var job = new ScheduledJob(jobId, partition);
        boolean alreadyScheduled = !currentScheduledJobs.add(job);
        if(alreadyScheduled)
            throw new IllegalStateException("Job " + jobId + " for partition " + partition +
                    " was already scheduled on node " + uuid);
        return () -> currentScheduledJobs.remove(job);
    }

    public int getCurrentScheduledJobs() {
        return currentScheduledJobs.size();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (WorkerClient) obj;
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
                ", currentScheduledJobs='" + currentScheduledJobs + '\'' +
                '}';
    }

    private record ScheduledJob(int jobId, int partition) {
    }
}
