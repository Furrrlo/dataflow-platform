package it.polimi.ds.map_reduce.socket;

import it.polimi.ds.map_reduce.socket.packets.Packet;

/**
 * Record that wraps a packet, adding a sequence number
 * 
 * @param packet packet to wrap
 * @param seqN sequence number
 *
 * @see it.polimi.ds.map_reduce.socket.SocketManager#send(Packet, Class)
 */
record SimpleSeqPacket(Packet packet, long seqN) implements SeqPacket {
}
