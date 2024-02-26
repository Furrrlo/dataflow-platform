package it.polimi.ds.dataflow;

import org.jspecify.annotations.Nullable;

import java.io.Serializable;

public record Tuple2(Object key, @Nullable Object value) implements Serializable {
}
