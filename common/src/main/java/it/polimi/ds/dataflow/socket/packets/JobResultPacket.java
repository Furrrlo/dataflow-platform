package it.polimi.ds.dataflow.socket.packets;

import it.polimi.ds.dataflow.Tuple2;

import java.util.List;

public record JobResultPacket(List<Tuple2> result) implements C2SAckPacket {
}
