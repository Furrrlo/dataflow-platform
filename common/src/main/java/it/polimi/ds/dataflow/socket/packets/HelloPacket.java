package it.polimi.ds.dataflow.socket.packets;

import java.io.Serializable;
import java.util.List;
import java.util.SequencedCollection;
import java.util.UUID;

public record HelloPacket(UUID uuid,
                          String dfsNodeName,
                          SequencedCollection<PreviousJob> previousJobs) implements C2SPacket {

    public HelloPacket(UUID uuid, String dfsNodeName, SequencedCollection<PreviousJob> previousJobs) {
        this.uuid = uuid;
        this.dfsNodeName = dfsNodeName;
        this.previousJobs = List.copyOf(previousJobs);
    }

    public record PreviousJob(int jobId, int partition) implements Serializable {
    }
}
