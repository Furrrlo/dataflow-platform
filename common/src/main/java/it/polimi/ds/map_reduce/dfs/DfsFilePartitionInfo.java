package it.polimi.ds.map_reduce.dfs;

public record DfsFilePartitionInfo(String fileName, int partition, String dfsNodeName, boolean isLocal) {
}
