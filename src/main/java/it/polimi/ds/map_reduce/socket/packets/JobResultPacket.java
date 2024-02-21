package it.polimi.ds.map_reduce.socket.packets;

import it.polimi.ds.map_reduce.Tuple2;

import java.util.List;

public record JobResultPacket(List<Tuple2> result) implements C2SAckPacket {
}
