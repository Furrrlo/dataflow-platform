package it.polimi.ds.map_reduce.socket.packets;

public record CreateFilePartitionPacket(String fileName, int partitionNum) implements S2CPacket {
}
