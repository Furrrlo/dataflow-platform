package it.polimi.ds.dataflow.js;

import java.io.Serializable;

public record Op(OpKind kind, String body) implements Serializable {
}
