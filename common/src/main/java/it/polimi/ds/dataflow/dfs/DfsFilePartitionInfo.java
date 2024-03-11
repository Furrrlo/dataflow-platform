package it.polimi.ds.dataflow.dfs;

public record DfsFilePartitionInfo(
        String fileName,
        String partitionFileName,
        int partition,
        String dfsNodeName,
        boolean isLocal
) {
}
