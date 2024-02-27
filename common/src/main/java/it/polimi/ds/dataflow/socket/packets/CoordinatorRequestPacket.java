package it.polimi.ds.dataflow.socket.packets;

public sealed interface CoordinatorRequestPacket
        extends S2CPacket
        permits CreateFilePartitionPacket, ScheduleJobPacket {
}
