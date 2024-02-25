package it.polimi.ds.map_reduce.js;

import java.io.Serializable;

public record Op(OpKind kind, String body) implements Serializable {
}
