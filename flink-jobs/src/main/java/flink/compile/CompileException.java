package flink.compile;

/**
 * Raised when a rule or aggregation definition cannot be compiled. The running job keeps the
 * previous version of the definition and reports the error to the DLQ instead of failing.
 */
public class CompileException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public CompileException(String message) {
        super(message);
    }
}
