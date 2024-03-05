package it.polimi.ds.dataflow.socket.packets;

import java.util.List;
import java.util.SequencedCollection;
import java.util.UUID;

public record HelloPacket(UUID uuid,
                          String dfsNodeName,
                          SequencedCollection<PreviousJob> previousJobs) implements C2SPacket {

    public HelloPacket {
        previousJobs = List.copyOf(previousJobs);
    }

    public record PreviousJob(int jobId, int partition) {
    }
}
