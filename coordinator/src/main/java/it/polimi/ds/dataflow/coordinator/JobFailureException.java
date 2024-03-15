package it.polimi.ds.dataflow.coordinator;

@SuppressWarnings({
        "serial", // Don't care about this being serializable
        "RedundantSuppression" // Javac complains about serial, IntelliJ about the suppression
})
public class JobFailureException extends Exception {

    public JobFailureException(String message) {
        super(message);
    }

    public JobFailureException(String message, Throwable cause) {
        super(message, cause);
    }

    public JobFailureException(Throwable cause) {
        super(cause);
    }
}
