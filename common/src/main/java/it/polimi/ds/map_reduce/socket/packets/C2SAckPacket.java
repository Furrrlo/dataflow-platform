package it.polimi.ds.map_reduce.socket.packets;

/** Client-to-server packet which acknowledges another one previously sent in the opposite direction */
public interface C2SAckPacket extends C2SPacket, AckPacket {
}
