package org.garret.perst.continuous;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.io.IOException;
import org.apache.lucene.search.Hits;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.garret.perst.*;

class FullTextSearchIterator implements Iterator<FullTextSearchResult>
{ 
    public FullTextSearchIterator(Hits hits, Storage storage, VersionSelector selector) 
    {
        this.hits = hits;
        this.selector = selector;
        this.storage = storage;
        TransactionContext ctx = CDatabase.getTransactionContext();
        transId = ctx != null ? ctx.transId : TransactionContext.IMPLICIT_TRANSACTION_ID; 
    }
    
    public boolean hasNext() 
    { 
        if (current == null) { 
            try { 
                while (i < hits.length()) { 
                    Document doc = hits.doc(i++);
                    Field f = doc.getField("Oid");
                    if (f == null) { 
                        continue;
                    }
                    CVersion v = (CVersion)storage.getObjectByOID(Integer.parseInt(f.stringValue()));                
                    switch (selector.kind) { 
                    case Current:
                        if (v.getVersionHistory().isCurrentForTransaction(v, transId)) { 
                            break;
                        }
                        continue;
                    case All:
                        if (v.history.limited) { 
                            if (v.getVersionHistory().isCurrentForTransaction(v, transId)) { 
                                break;
                            }
                        } else {
                            if (v.transId <= transId) { 
                                break;
                            }
                        }
                        continue;
                    case TimeSlice:
                        if (v.transId <= transId 
                            && !v.history.limited
                            && (selector.from == null || v.date.compareTo(selector.from) >= 0)
                            && (selector.till == null || v.date.compareTo(selector.till) <= 0))
                        {
                            break;
                        }
                        continue;
                    }
                    current = new FullTextSearchResult(v, hits.score(i-1));
                    break;
                }
            } catch (IOException x) { 
                throw new IOError(x);
            }
        }
        return current != null;
    }

    public FullTextSearchResult next() 
    {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        FullTextSearchResult obj = current;
        current = null;
        return obj;
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }

    private Hits hits;
    private Storage storage;
    private VersionSelector selector;
    private long transId;
    private FullTextSearchResult current;
    private int i;
}
