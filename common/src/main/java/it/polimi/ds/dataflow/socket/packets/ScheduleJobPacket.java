package it.polimi.ds.dataflow.socket.packets;

import it.polimi.ds.dataflow.Tuple2;
import it.polimi.ds.dataflow.js.Op;

import java.util.List;

public record ScheduleJobPacket(int jobId, List<Op> ops, List<Tuple2> data) implements S2CPacket {
}
