package it.polimi.ds.map_reduce.socket.packets;

import it.polimi.ds.map_reduce.Tuple2;
import it.polimi.ds.map_reduce.js.Op;

import java.util.List;

public record ScheduleJobPacket(int jobId, List<Op> ops, List<Tuple2> data) implements S2CPacket {
}
