package org.garret.perst.continuous;
import org.garret.perst.*;

/**
 * Full text search result
 */
public class FullTextSearchResult 
{
    /**
     * Document version
     */
    public CVersion getVersion() { 
        return v;
    }

    /**
     * Document score
     */
    public float    getScore() { 
        return score;
    }

    FullTextSearchResult(CVersion v, float score) { 
        this.v = v;
        this.score = score;
    }

    CVersion v;
    float score;
    
}