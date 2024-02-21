package it.polimi.ds.map_reduce.socket.packets;

import it.polimi.ds.map_reduce.js.Op;

import java.util.List;

public record ScheduleJobPacket(int jobId, List<Op> ops) implements S2CPacket {
}
