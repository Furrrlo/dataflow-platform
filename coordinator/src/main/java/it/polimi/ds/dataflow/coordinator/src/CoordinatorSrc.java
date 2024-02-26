package it.polimi.ds.dataflow.coordinator.src;

import it.polimi.ds.map_reduce.src.Src;

public sealed interface CoordinatorSrc extends Src permits LinesSrc, CsvSrc, DfsSrc {

    enum Kind {
        LINES("lines"),
        CSV("csv"),
        DFS("dfs");

        private final String methodIdentifier;

        Kind(String methodIdentifier) {
            this.methodIdentifier = methodIdentifier;
        }

        public String getMethodIdentifier() {
            return methodIdentifier;
        }
    }
}
