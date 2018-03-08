package org.garret.perst.continuous;

/**
 * Base class for all exceptions thrown by this package
 */
public class ContinuousException extends RuntimeException 
{
    ContinuousException(Throwable cause) {
        super(cause);
    }

    ContinuousException(String message) { 
        super(message);
    }
}