package org.garret.perst.continuous;

import java.util.*;
import org.garret.perst.*;

/**
 * Collection of versions of versioned object.
 * Versioned object should be accessed only through version history object.
 * Instead of storing direct reference to CVerson in some component of some other persistent object, 
 * it is necessary to store reference to it's CVersionHistory. 
 * Version is version history are assigned sequential numbers (version identifiers) starting from 1. 
 */
public class CVersionHistory<V extends CVersion> extends Persistent implements Iterable<V>
{
    /**
     * Get current version in the version history for the current transaction.
     * @return current version for the current transaction or null if none 
     * (version history was create after transaction has been started)
     */
    public synchronized V getCurrent() 
    {
        TransactionContext ctx = CDatabase.getTransactionContext(); 
        if (ctx == null) { 
            return versions.get(versions.size()-1);
        }
        CVersion wc = ctx.getWorkingCopy(this);
        return (wc != null) ? (V)wc : getCurrent(ctx.transId);
    }

    /**
     * Get current version in the version history for the specified transaction.
     * @param transId identifier of previosly committed transaction which can be obtained using CVersion.getTransactionId() method
     * @return current version for the specified transaction or null if none 
     * (version history was create after transaction has been started)
     */
    public synchronized V getCurrent(long transId) 
    { 
        for (int i = versions.size(); --i >= 0;) { 
            V v = versions.get(i);
            if (v.transId <= transId) { 
                return v;
            }
        }
        return null;
    }

    /**
     * Get last version in the version history
     * @return latest version in the version history
     */
    public synchronized V getLast() {
        return versions.get(versions.size()-1);
    }

    /**
     * Get working copy for this version history
     * @return existed working copy of some version in this version history or newly create working copy of the current version
     */
    public synchronized V update() 
    { 
        return (V)CDatabase.getWriteTransactionContext().update(this);
    }

    /**
     * Create new working copy of the current version and mark it as been deleted
     * @exception NotCurrentVersionException is thrown if working copy was created not from the current version
     */
    public synchronized void delete() 
    { 
        CVersion wc = update();
        if (wc.history.isCurrentForTransaction(wc, wc.transId)) {
            wc.flags |= CVersion.DELETED;
        } else { 
            throw new NotCurrentVersionException(wc);
        }
    }

    /**
     * Get version with specified ID
     * @param id version identifier (starting from 1)
     * @return version with this ID
     */
    public synchronized V get(int id) { 
        return versions.get(id-1);
    }

    /**
     * Remove version with specified identifier.
     * This methodd should be used with care since removing version while it is accessed buy some other 
     * thread may cause unpredictable behaviour
     */
    public synchronized void remove(int id) { 
        versions.remove(id-1);
    }

    /**
     * Get latest version before specified date
     * @param timestamp deadline, if <code>null</code> then the latest version in the version history will be returned
     * @return version with the largest timestamp less than or equal to specified <code>timestamp</code>
     */
    public synchronized V getLatestBefore(Date timestamp) 
    { 
        if (timestamp == null) { 
            return versions.get(versions.size()-1);
        }
        int l = 0, n = versions.size(), r = n;
        long t = timestamp.getTime()+1;
        while (l < r) { 
            int m = (l + r) >> 1;
            if (versions.get(m).getDate().getTime() < t) { 
                l = m + 1;
            } else { 
                r = m;
            }
        }
        return r > 0 ? versions.get(r-1) : null;
    }
    
    /**
     * Get earliest version after specified date
     * @param timestamp deadline, if <code>null</code> then root version will be returned
     * @return version with the smallest timestamp greater than or equal to specified <code>timestamp</code>
     */
    public synchronized V getEarliestAfter(Date timestamp)
    {
        if (timestamp == null) { 
            return versions.get(0);
        }
        int l = 0, n = versions.size(), r = n;
        long t = timestamp.getTime();
        while (l < r) { 
            int m = (l + r) >> 1;
            if (versions.get(m).getDate().getTime() < t) { 
                l = m + 1;
            } else { 
                r = m;
            }
        }
        return r < n ? versions.get(r) : null;
    }

    /**
     * Get number of versions in the version history
     * @return version history size
     */
    public synchronized int getNumberOfVersions() 
    { 
        return versions.size();
    }

    /**
     * Get all versions in the version history
     * @return array of versions sorted by date
     */
    public synchronized CVersion[] getAllVersions()
    {
        return versions.toArray(new CVersion[versions.size()]);
    }

    /**
     * Get iterator through all version in version history
     * Iteration is started from the root version and performed in direction of increaing
     * version timestamp
     * @return version iterator
     */
    public synchronized Iterator<V> iterator() 
    { 
        return versions.iterator();
    }

    synchronized void add(V v) 
    { 
        versions.add(v);
        store();
    }

    synchronized boolean isCurrentForTransaction(CVersion v, long transId) 
    { 
        return v.transId <= transId && (v.id == versions.size() || versions.get(v.id).transId > transId);
    }
        
    CVersionHistory(Storage storage, boolean limited) 
    { 
        versions = storage.<V>createLink(1);
        this.limited = limited;
    }

    Link<V> versions;
    boolean limited;
}