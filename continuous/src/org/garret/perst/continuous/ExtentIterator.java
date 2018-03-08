package org.garret.perst.continuous;

import java.util.*;
import org.garret.perst.*;

class ExtentIterator<T> extends IterableIterator<T> 
{ 
    ExtentIterator(Iterator<CVersionHistory> iterator, IResource resource, VersionSelector selector) {
        this.iterator = iterator;
        this.resource = resource;
        this.selector = selector;
        TransactionContext ctx = CDatabase.getTransactionContext();
        transId = ctx != null ? ctx.transId : TransactionContext.IMPLICIT_TRANSACTION_ID; 
    }
    
    public boolean hasNext() 
    { 
        if (currVersion == null) {
            CVersion v;
            resource.sharedLock();
            try { 
                switch (selector.kind) { 
                case Current:
                    while (iterator.hasNext()) { 
                        CVersionHistory vh = iterator.next();
                        v = vh.getCurrent(transId);
                        if (v != null && !v.isDeletedVersion()) { 
                            currVersion = v;
                            break;
                        }
                    } 
                    break;
                case All:
                    if (currVersionHistory != null) { 
                        while (currVersionNumber < currVersionHistory.getNumberOfVersions()) { 
                            v = currVersionHistory.get(++currVersionNumber);
                            if (v.transId <= transId) { 
                                if (!v.isDeletedVersion()) { 
                                    currVersion = v;
                                    break;
                                }
                            } else { 
                                break;
                            }
                        } 
                    }
                    if (currVersion == null) { 
                        while (iterator.hasNext()) { 
                            CVersionHistory vh = iterator.next();
                            if (vh.limited) {
                                v = vh.getCurrent(transId);
                                if (v != null && !v.isDeletedVersion()) { 
                                    currVersion = v;
                                    break;
                                }
                            } else { 
                                v = vh.get(CVersion.FIRST_VERSION_ID);
                                if (v.transId <= transId) { 
                                    currVersionHistory = vh;                            
                                    currVersion = v;
                                    currVersionNumber = CVersion.FIRST_VERSION_ID;
                                    break;
                                }
                            }
                        }
                    }
                    break;
                case TimeSlice:
                    if (currVersionHistory != null) { 
                        while (currVersionNumber < currVersionHistory.getNumberOfVersions()) { 
                            v = currVersionHistory.get(++currVersionNumber);
                            if (v.transId <= transId) { 
                                if (!v.isDeletedVersion()) { 
                                    if (selector.till == null || v.date.compareTo(selector.till) <= 0) {
                                        currVersion = v;
                                    }
                                    break;
                                }
                            } else { 
                                break;
                            }
                        } 
                    }
                    if (currVersion == null) { 
                        while (iterator.hasNext()) { 
                            CVersionHistory vh = iterator.next();
                            if (vh.limited) {
                                continue;
                            }
                            int last = vh.getNumberOfVersions();
                            int id = 0;
                            if (selector.from != null) { 
                                int l = 0, r = last;
                                while (l < r) { 
                                    int m = (l + r) >>> 1;
                                    v = vh.get(m + 1);
                                    int diff = v.date.compareTo(selector.from);
                                    if (diff <= 0) { 
                                        l = m + 1;
                                    } else { 
                                        r = m;
                                    }
                                }
                                id = r;
                            }
                            v = null;
                            while (id < last && (v = vh.get(++id)).isDeletedVersion()) { 
                                v = null;
                            }
                            if (v != null && v.transId <= transId
                                && (selector.till == null || v.date.compareTo(selector.till) <= 0) )
                            {
                                currVersionHistory = vh;                            
                                currVersion = v;
                                currVersionNumber = id;
                                break;
                            }
                        }
                    }
                    break;
                }
            } finally { 
                resource.unlock();
            } 
        }
        return currVersion != null;
    }

    public T next() 
    {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        CVersion obj = currVersion;
        currVersion = null;
        return (T)obj;
    }
    
    public void remove() { 
        throw new UnsupportedOperationException();
    }
        

    private Iterator<CVersionHistory> iterator;
    private IResource resource;
    private VersionSelector selector;
    private long transId;
    private CVersion currVersion;
    private int currVersionNumber;
    private CVersionHistory currVersionHistory;
}

