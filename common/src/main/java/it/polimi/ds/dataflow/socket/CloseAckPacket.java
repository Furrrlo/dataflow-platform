package it.polimi.ds.dataflow.socket;

import it.polimi.ds.dataflow.socket.packets.C2SAckPacket;
import it.polimi.ds.dataflow.socket.packets.S2CAckPacket;

/** Acknowledge that {@link it.polimi.ds.dataflow.socket.ClosePacket} was received and this side is also starting to close */
record CloseAckPacket() implements S2CAckPacket, C2SAckPacket {
}
