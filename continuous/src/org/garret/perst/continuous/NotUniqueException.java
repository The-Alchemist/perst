package org.garret.perst.continuous;

/**
 * Exception thrown when unique constraint violation is detected during transaction commit
 */
public class NotUniqueException extends ContinuousException 
{
    /**
     * Get version which field contains not unique value
     */
    public CVersion getVersion() { 
        return v;
    }

    NotUniqueException(CVersion v) { 
        super("Unique constraint violated");
        this.v = v;
    }

    private CVersion v;
}
