package org.garret.perst.continuous;

/**
 * Exception thrown when application tries to insert in the database object which is already part of some other version history
 */
public class ObjectAlreadyInsertedException extends ContinuousException 
{
    /**
     * Version which insertion attempt was rejected
     */
    public CVersion getVersion() { 
        return v;
    }
 
    ObjectAlreadyInsertedException(CVersion v) { 
        super("Transaction already started exception");
        this.v = v;
    }

    CVersion v;
}
