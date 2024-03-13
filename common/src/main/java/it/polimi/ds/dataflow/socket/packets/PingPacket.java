package it.polimi.ds.dataflow.socket.packets;

public record PingPacket() implements CoordinatorRequestPacket{
    public static final int TIMEOUT_MILLIS = 50000;
}
