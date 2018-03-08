package org.garret.perst.continuous;

import java.util.*;
import org.garret.perst.*;

class IndexIterator<T> extends IterableIterator<T> 
{
    IndexIterator(Iterator<VersionHistorySegment> iterator, IResource resource, VersionSelector selector) 
    {
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
                        VersionHistorySegment segment = iterator.next();
                        v = segment.getCurrentVersion(transId);
                        if (v != null) { 
                            currVersion = v;
                            break;
                        }
                    } 
                    break;
                case All:
                    if (currSegment != null) { 
                        while (currVersionNumber < currSegment.till) { 
                            v = currSegment.vh.get(++currVersionNumber);
                            if (v.transId <= transId) { 
                                currVersion = v;
                                break;
                            }
                        } 
                    }
                    if (currVersion == null) { 
                        while (iterator.hasNext()) { 
                            VersionHistorySegment segment = iterator.next();
                            if (segment.vh.limited) { 
                                v = segment.getCurrentVersion(transId);
                                if (v != null) { 
                                    currVersion = v;
                                    break;
                                }
                            } else {
                                v = segment.vh.get(segment.from);
                                if (v.transId <= transId) { 
                                    currSegment = segment;
                                    currVersion = v;
                                    currVersionNumber = segment.from;
                                    break;
                                }
                            }
                        }
                    }
                    break;
                case TimeSlice:
                    if (currSegment != null) { 
                        while (currVersionNumber < currSegment.till) { 
                            v = currSegment.vh.get(++currVersionNumber);
                            if (v.transId <= transId
                                && (selector.till == null || v.date.compareTo(selector.till) <= 0)) 
                            { 
                                currVersion = v;
                            }
                            break;
                        } 
                    }
                    if (currVersion == null) { 
                        while (iterator.hasNext()) { 
                            VersionHistorySegment segment = iterator.next();
                            if (segment.vh.limited) { 
                                continue;
                            }
                            int from = segment.from;
                            int till = segment.till;
                            if (selector.from != null) { 
                                int l = from, r = till+1;
                                while (l < r) { 
                                    int m = (l + r) >>> 1;
                                    v = segment.vh.get(m);
                                    int diff = v.date.compareTo(selector.from);
                                    if (diff <= 0) { 
                                        l = m + 1;
                                    } else { 
                                        r = m;
                                    }
                                }
                                from = r;
                            }
                            if (from <= till) { 
                                v = segment.vh.get(from);
                                if (v.transId <= transId 
                                    && (selector.till == null || v.date.compareTo(selector.till) <= 0)) 
                                {
                                    currSegment = segment;
                                    currVersion = v;
                                    currVersionNumber = from;
                                    break;
                                }
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

    private Iterator<VersionHistorySegment> iterator;
    private IResource resource;
    private VersionSelector selector;
    private CVersion currVersion;
    private VersionHistorySegment currSegment;
    private int currVersionNumber;
    private long transId;
}