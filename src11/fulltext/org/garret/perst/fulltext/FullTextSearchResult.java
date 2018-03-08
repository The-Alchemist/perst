package org.garret.perst.fulltext;

import org.garret.perst.*;

/**
 * Full text search result
 */
public class FullTextSearchResult { 
    /**
     * Estimation of total number of documents in the index matching this query.
     * Full text search query result is usually limited by number of returned documents
     * and query execution time. So there are can be more documents in the index matching this query than 
     * actually returned. This field provides estimation for total number of documents matching the query.
     */
    public int estimation;

    /**
     * Full text search result hits
     */
    public FullTextSearchHit[] hits;
    
    /** 
     * Merge results of two searches
     * @param another Full text search result to merge with this result
     * @return Result set containing documents present in both result sets
     */
    public FullTextSearchResult merge(FullTextSearchResult another) {
        if (hits.length == 0 || another.hits.length == 0) {
            return new FullTextSearchResult(new FullTextSearchHit[0], 0);
        }
        FullTextSearchHit[] joinHits = new FullTextSearchHit[hits.length + another.hits.length];
        System.arraycopy(hits, 0, joinHits, 0, hits.length);
        System.arraycopy(another.hits, 0, joinHits, hits.length, another.hits.length);
        Arrays.sort(joinHits, new Comparator() { 
            public int compare(Object o1, Object o2) {
                return ((FullTextSearchHit)o1).oid - ((FullTextSearchHit)o2).oid;
            }
        });
        int n = 0;
        for (int i = 1; i < joinHits.length; i++) { 
            if (joinHits[i].oid == joinHits[i-1].oid) {                    
                joinHits[n++] = new FullTextSearchHit(joinHits[i].storage, joinHits[i].oid, joinHits[i-1].rank + joinHits[i].rank);                    
                i += 1;
            } 
        }
        FullTextSearchHit[] mergeHits = new FullTextSearchHit[n];
        System.arraycopy(joinHits, 0, mergeHits, 0, n);
        Arrays.sort(joinHits);
        return new FullTextSearchResult(joinHits, Math.min(estimation*n/hits.length, another.estimation*n/another.hits.length));
    }

    public FullTextSearchResult(FullTextSearchHit[] hits, int estimation) { 
        this.hits = hits;
        this.estimation = estimation;
    }
}