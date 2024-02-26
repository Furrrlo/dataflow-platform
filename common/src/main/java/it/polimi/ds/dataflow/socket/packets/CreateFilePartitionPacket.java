package it.polimi.ds.dataflow.socket.packets;

public record CreateFilePartitionPacket(String fileName, int partitionNum) implements S2CPacket {
}
