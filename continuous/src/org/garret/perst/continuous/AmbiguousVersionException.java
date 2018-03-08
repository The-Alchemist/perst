package org.garret.perst.continuous;

/**
 * Exception thrown when application tries to update more than one version from the same version history within one transaction
 */
public class AmbiguousVersionException extends ContinuousException 
{
    /**
     * Get version which update attempt was rejected
     */
    public CVersion getVersion() { 
        return v;
    }

    AmbiguousVersionException(CVersion v) { 
        super("Attempt to update more than one version from the same version history in one transaction");
        this.v = v;
    }

    private CVersion v;
}
