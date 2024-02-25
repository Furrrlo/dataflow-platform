package it.polimi.ds.map_reduce.socket.packets;

import java.io.Serializable;

/**
 * Packet that can be sent over socket.
 *
 * @see it.polimi.ds.map_reduce.socket.SocketManager#send(Packet, Class)
 */
public interface Packet extends Serializable {
}
