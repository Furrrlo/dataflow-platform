package it.polimi.ds.map_reduce.socket;

import it.polimi.ds.map_reduce.socket.packets.C2SAckPacket;
import it.polimi.ds.map_reduce.socket.packets.S2CAckPacket;

/**
 * Empty ack packet
 * This will also be wrapped in a {@link SeqAckPacket}, but it's seqN is not used.
 *
 * @see SocketManager.PacketReplyContext#ack()
 */
record SimpleAckPacket() implements S2CAckPacket, C2SAckPacket {
}
