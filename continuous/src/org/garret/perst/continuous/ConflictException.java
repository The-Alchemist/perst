package org.garret.perst.continuous;

/**
 * Exception thrown when conflict is detected during transaction commit
 */
public class ConflictException extends ContinuousException 
{
    CVersion v;
    
    /**
     * Get version which is conflicted with version previously committed by another transaction
     */
    public CVersion getVersion() { 
        return v;
    }
    
    ConflictException(CVersion v) { 
        super("Version conflict detected");
        this.v = v;
    }
}