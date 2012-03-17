package de.tuberlin.dima.presslufthammer.query.sema;

/**
 * For errors that occur during semantic analysis.
 * 
 * @author Aljoscha Krettek
 *
 */
public class SemaError extends Exception {

    private static final long serialVersionUID = -2135009711441923775L;

    public SemaError(String message) {
        super(message);
    }
}
