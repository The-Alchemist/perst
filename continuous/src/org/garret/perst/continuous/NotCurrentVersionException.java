package org.garret.perst.continuous;

/**
 * Exception thrown when application tries to delete non-current version 
 */
public class NotCurrentVersionException extends ContinuousException 
{
    /**
     * Get version which delete attempt was rejected
     */
    public CVersion getVersion() { 
        return v;
    }

    NotCurrentVersionException(CVersion v) { 
        super("Attempt to delete non-current version");
        this.v = v;
    }

    private CVersion v;
}
