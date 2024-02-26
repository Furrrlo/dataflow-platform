package it.polimi.ds.dataflow.dfs;

public record DfsFilePartitionInfo(String fileName, int partition, String dfsNodeName, boolean isLocal) {
}
