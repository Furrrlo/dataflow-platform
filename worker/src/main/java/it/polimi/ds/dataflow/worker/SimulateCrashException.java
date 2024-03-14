package it.polimi.ds.dataflow.worker;

@SuppressWarnings({
        "serial", // Don't care about this being serializable
        "RedundantSuppression" // Javac complains about serial, IntelliJ about the suppression
})
public class SimulateCrashException extends RuntimeException {

    public SimulateCrashException() {
    }

    public SimulateCrashException(Throwable cause) {
        super(cause);
    }
}
