package it.polimi.ds.map_reduce.socket.packets;

/** Server-to-client packet which acknowledges another one previously sent in the opposite direction */
public interface S2CAckPacket extends S2CPacket, AckPacket {
}
