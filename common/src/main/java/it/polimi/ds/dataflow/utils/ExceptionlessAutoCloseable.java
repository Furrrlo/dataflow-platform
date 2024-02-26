package it.polimi.ds.dataflow.utils;

public interface ExceptionlessAutoCloseable extends AutoCloseable {

    @Override
    void close();
}
