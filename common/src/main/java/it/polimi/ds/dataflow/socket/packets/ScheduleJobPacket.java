package it.polimi.ds.dataflow.socket.packets;

import it.polimi.ds.dataflow.js.Op;

import java.util.List;

public record ScheduleJobPacket(
        int jobId,
        List<Op> ops,
        int partitions,
        String dfsSrcFileName, int partition,
        String dfsDstFileName,
        boolean reshuffle
) implements CoordinatorRequestPacket {
}
