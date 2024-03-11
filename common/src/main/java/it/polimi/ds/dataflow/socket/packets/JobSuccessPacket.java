package it.polimi.ds.dataflow.socket.packets;

public record JobSuccessPacket(String dstDfsPartitionFileName) implements JobResultPacket {
}
