package org.garret.perst.continuous;

import org.garret.perst.*;
import java.util.Date;

/**
 * Base class for version of versioned object. All versions are kept in linear version history.
 */
public class CVersion extends Persistent
{
    public static final int FIRST_VERSION_ID = 1;

    /**
     * Get version history containing this version
     */
    public <T extends CVersion> CVersionHistory<T> getVersionHistory() { 
        return history;
    }

    /**
     * Create working copy of this version
     * @return working copy of this version
     * @exception AmbiguousVersionException when some other version from the same version history was already updated by the current transaction
     * @exception TransactionNotStartedException if transaction was not started by this thread using CDatabase.beginTransaction
     */
    public <T extends CVersion> T update() {         
        return (T)(((flags & WORKING_COPY) != 0) ? this : CDatabase.getWriteTransactionContext().update(this));
    }

    /**
     * Create working copy of this version and mark it as been deleted
     * @exception NotCurrentVersionException is thrown if this version is not current in version history
     * @exception TransactionNotStartedException if transaction was not started by this thread using CDatabase.beginTransaction
     */
    public void delete() 
    { 
        CVersion wc = ((flags & WORKING_COPY) != 0) ? this : CDatabase.getWriteTransactionContext().update(this);
        if (history.isCurrentForTransaction(wc, wc.transId)) {
            wc.flags |= DELETED;
        } else { 
            throw new NotCurrentVersionException(this);
        }
    }

    /**
     * Get date of version creation 
     * @return date when this version was created
     */
    public Date getDate() { 
        return date;
    }

    /**
     * Get identifier of the version. Version is version history are assigned sequential numbers starting from 1. 
     * @return version identifier automatically assigned by system.
     */
    public int getId() { 
        return id;
    }


    /**
     * Get identifier of transaction which has committed this version.
     * This identifier can be passed in CDatabase.beginTransaction method to obtain snapshot of the database at the 
     * moment of this transaction execution
     * @return identifier of transaction committed this version
     */
    public long getTransactionId() { 
        return transId;
    }

    /**
     * Check if specified version is the last version in the version history
     * @return true if it the last version in version history
     */
    public boolean isLastVersion() { 
        return id == history.getNumberOfVersions();
    }

    /**
     * Check if version corresponds to the deleted object. Object deletion is handled by marking version with DELETED 
     * flag. It is possible to create working copy of the deleted version and commit it - this is how undelete can 
     * be performed. So it can happen that multiple versions in version history are marked as been deleted, if
     * delete/undelete operations were repeated several times. 
     * @return true if the version is marked as deleted
     */
    public boolean isDeletedVersion() { 
        return (flags & CVersion.DELETED) != 0;
    }

    /**
     * Constructor of root version. All other versions should be created using 
     * <code>CVersionHistory.update</code> method
     */
    protected CVersion(Storage storage) {
        super(storage);
    }

    /**
     * Constructor of root version. All other versions should be created using 
     * <code>CVersionHistory.update</code> method
     */
    protected CVersion() {
    }

    CVersion createWorkingCopy(TableDescriptor desc) 
    { 
       try { 
           CVersion wc = (CVersion)clone();
           if (desc != null) { 
               desc.cloneFields(wc);
           }
           wc.clearState();
           wc.flags = (byte)WORKING_COPY;
           return wc;
        } catch (CloneNotSupportedException x) { 
            // Could not happen since we clone ourselves
            throw new AssertionFailed("Clone not supported");
        }
    }
           
    int  id; 
    long transId;
    Date date;
    CVersionHistory history;

    static final int NEW = 1;
    static final int DELETED = 2;
    static final int WORKING_COPY = 4;
    byte flags;
}
