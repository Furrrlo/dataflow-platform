package it.polimi.ds.dataflow.socket;

import it.polimi.ds.dataflow.socket.packets.Packet;

/**
 * Record that wraps a packet, adding a sequence number
 * 
 * @param packet packet to wrap
 * @param seqN sequence number
 *
 * @see it.polimi.ds.dataflow.socket.SocketManager#send(Packet, Class)
 */
record SimpleSeqPacket(Packet packet, long seqN) implements SeqPacket {
}
