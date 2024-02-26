package it.polimi.ds.dataflow.socket.packets;

import java.io.Serializable;

/**
 * Packet that can be sent over socket.
 *
 * @see it.polimi.ds.dataflow.socket.SocketManager#send(Packet, Class)
 */
public interface Packet extends Serializable {
}
