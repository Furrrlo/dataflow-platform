package it.polimi.ds.dataflow.socket.packets;

import it.polimi.ds.dataflow.js.Op;

import java.util.List;

public record ScheduleJobPacket(
        int jobId,
        List<Op> ops,
        String dfsSrcFileName, int partition,
        String dfsDstFileName
) implements CoordinatorRequestPacket {
}
