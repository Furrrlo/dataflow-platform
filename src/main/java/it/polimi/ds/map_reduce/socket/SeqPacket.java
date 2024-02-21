package it.polimi.ds.map_reduce.socket;

import it.polimi.ds.map_reduce.socket.packets.Packet;

import java.io.Serializable;

/**
 * Interface that wraps a packet and adds a sequence number
 * Its implementors are the only object that can actually be sent over sockets
 *
 * @see it.polimi.ds.map_reduce.socket.SocketManager#send(Packet, Class)
 */
public sealed interface SeqPacket extends Serializable permits SimpleSeqPacket, SeqAckPacket {

    /** Returns the wrapped packet */
    Packet packet();

    /** Returns the sequence number */
    long seqN();
}
