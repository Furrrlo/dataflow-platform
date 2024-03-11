package it.polimi.ds.dataflow.utils;

@SuppressWarnings({
        "serial", // Don't care about this being serializable
        "RedundantSuppression" // Javac complains about serial, IntelliJ about the suppression
})
@SuppressFBWarnings("SE_BAD_FIELD") // Don't care about this being serializable
public class FastIllegalStateException extends IllegalStateException {

    public FastIllegalStateException(String s) {
        super(s);
    }

    @Override
    public Throwable fillInStackTrace() {
        return this;
    }
}
