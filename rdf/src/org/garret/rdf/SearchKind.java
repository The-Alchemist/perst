package org.garret.rdf;

/**
 * Which verions of the object should be inspected
 */
public class SearchKind 
{
    /**
     * Latest version in version history
     */
    public static final SearchKind LatestVersion = new SearchKind("Latest version");
    
    /**
     * All versions in version history
     */
    public static final SearchKind AllVersions = new SearchKind("All versions");

    /**
     * Latest version before sepcified timestamp
     */
    public static final SearchKind LatestBefore = new SearchKind("Latest before");

    /** 
     * Oldest version after sepcified timestamp
     */
    public static final SearchKind OldestAfter = new SearchKind("Oldest after");

    private SearchKind(String kind) {
        this.kind = kind;
    }

    public String toString() { 
        return kind;
    }

    private String kind;
}
