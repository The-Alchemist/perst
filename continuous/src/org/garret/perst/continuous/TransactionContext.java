package org.garret.perst.continuous;

import java.util.*;

class TransactionContext 
{
    long transId;
    long seqNo;
    HashMap<CVersionHistory,CVersion> workingCopies;
    CDatabase db;
 
    Collection<CVersion> getWorkingCopies() { 
        return workingCopies.values();
    }
    
    CVersion getWorkingCopy(CVersionHistory vh) { 
        return workingCopies.get(vh);
    }
    

    static final long IMPLICIT_TRANSACTION_ID = Long.MAX_VALUE;
    
    void insert(CVersion v) 
    { 
        if (transId == IMPLICIT_TRANSACTION_ID) { 
            throw new TransactionNotStartedException();
        }
        if (v.history != null) { 
            throw new ObjectAlreadyInsertedException(v);
        }
        TableDescriptor desc = db.getTable(v.getClass());
        CVersionHistory vh = new CVersionHistory(db.getStorage(), desc.notVersioned);
        v.history = vh;
        v.flags = (byte)(CVersion.NEW|CVersion.WORKING_COPY);
        v.id = 1;
        workingCopies.put(vh, v);            
    }

    CVersion update(CVersionHistory vh) 
    { 
        if (transId == IMPLICIT_TRANSACTION_ID) { 
            throw new TransactionNotStartedException();
        }
        CVersion v = workingCopies.get(vh);
        if (v == null) { 
            TableDescriptor desc = db.lookupTable(vh.getClass());
            v = vh.getCurrent(transId).createWorkingCopy(desc);
            v.transId = transId;
            workingCopies.put(vh, v);            
        }
        return v;
    }

    CVersion update(CVersion v) 
    { 
        if (transId == IMPLICIT_TRANSACTION_ID) { 
            throw new TransactionNotStartedException();
        }
        CVersionHistory vh = v.history;
        CVersion wc = workingCopies.get(vh);
        if (wc == null) { 
            TableDescriptor desc = db.lookupTable(vh.getClass());
            wc = v.createWorkingCopy(desc);
            wc.transId = transId;
            workingCopies.put(vh, wc);            
        } else if (wc.id != v.id) { 
            throw new AmbiguousVersionException(v);
        }
        return wc;
    }

    void beginTransaction(long id, long no) 
    { 
        if (transId != IMPLICIT_TRANSACTION_ID && transId != id && workingCopies.size() != 0) { 
            throw new TransactionAlreadyStartedException();
        }
        transId = id;
        seqNo = no;
    }

    void endTransaction() 
    { 
        transId = IMPLICIT_TRANSACTION_ID;
        workingCopies.clear();
    }

    boolean isEmptyTransaction() { 
        return workingCopies.size() == 0;
    }

    TransactionContext(CDatabase db) 
    { 
        this.db = db;
        workingCopies = new HashMap<CVersionHistory,CVersion>();
    }
}
    