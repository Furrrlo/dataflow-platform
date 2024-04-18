package it.polimi.ds.dataflow.coordinator.src;

import it.polimi.ds.dataflow.src.Src;
import org.jetbrains.annotations.Unmodifiable;

import java.util.List;

public sealed interface CoordinatorSrc extends Src permits PartitionedCoordinatorSrc, NonPartitionedCoordinatorSrc {

    enum Kind {
        LINES("lines", 2, String.class, Integer.class),
        CSV("csv", 1, String.class, Integer.class, String.class),
        DFS("dfs", 1, String.class),
        REQUIRE("requireInput", 0, Void.class);

        public static final @Unmodifiable List<Kind> VALUES = List.of(values());

        private final String methodIdentifier;
        private final int minArgs;
        private final @Unmodifiable List<Class<?>> args;

        Kind(String methodIdentifier, int minArgs, Class<?>... args) {
            this.methodIdentifier = methodIdentifier;
            this.minArgs = minArgs;
            this.args = List.of(args);
        }

        public String getMethodIdentifier() {
            return methodIdentifier;
        }

        public int getMinArgs() {
            return minArgs;
        }

        public int getMaxArgs() {
            return args.size();
        }

        public @Unmodifiable List<Class<?>> getArgs() {
            return args;
        }
    }
}
