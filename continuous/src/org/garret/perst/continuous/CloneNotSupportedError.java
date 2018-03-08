package org.garret.perst.continuous;

/**
 * Exception rethrown when CloneNotSupportedException is catched
 */
public class CloneNotSupportedError extends ContinuousException 
{
    CloneNotSupportedError() { 
        super("Clone is not supported");
    }
}
