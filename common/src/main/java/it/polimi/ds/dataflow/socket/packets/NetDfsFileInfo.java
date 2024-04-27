package it.polimi.ds.dataflow.socket.packets;

import java.io.Serializable;
import java.util.List;

public record NetDfsFileInfo(
        String dfsFileName,
        int dfsFilePartitions,
        List<String> dfsPartitionNames
) implements Serializable {
}
