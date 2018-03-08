package org.garret.rdf;

import java.util.*;
import org.garret.perst.*;

/**
 * Class representing object (collection of its verions)
 */
public class VersionHistory extends Persistent { 
   /**
    * Object URI   
    */
    public String uri;

   /**
    * Vector of object versions (the latest version is the last element of the vector)   
    */
    public Link   versions;    
    
   /**
    * Type of this object   
    */
    public VersionHistory type;
  
   /**
    * Get latest version in version history   
    */
    public Thing getLatest() { 
        return (Thing)versions.get(versions.size()-1);
    }

   /**  
    * Get latest version in version history prior to the specified timestamp
    * 
    * @param timestamp timestamp
    * @return The latest version in version history prior to the specified timestamp 
    * or null if no such version is found
    */
    public Thing getLatestBefore(Date timestamp) {
        for (int i = versions.size(); --i >= 0;) { 
            Thing v = (Thing)versions.get(i);
            if (v.timestamp.compareTo(timestamp) <= 0) { 
                return v;
            }
        }
        return null;
    }
    
   /** 
    * Get oldest version in version history released after the specified timestamp
    * 
    * @param timestamp timestamp
    * @return The oldest version in version history released after the specified timestamp
    */
    public Thing getOldestAfter(Date timestamp) {
        for (int i = 0; i < versions.size(); i++) { 
            Thing v = (Thing)versions.get(i);
            if (v.timestamp.compareTo(timestamp) >= 0) { 
                return v;
            }
        }
        return null;  
    }

   /** 
    * Get version correponding to the specified search kind and timestamp
    * 
    * @param kind One of SearchKind.LAtestVersion, SearchKind.LatestBefore and SearchKind.OldestAfter
    * @param timestamp 
    * @return Version natching time criteria or null if not found
    */
    public Thing getVersion(SearchKind kind, Date timestamp) {
        if (kind == SearchKind.LatestVersion) { 
            return getLatest();
        } else if (kind == SearchKind.LatestBefore) {
            return getLatestBefore(timestamp);
        } else if (kind == SearchKind.OldestAfter) { 
            return getOldestAfter(timestamp);
        } else { 
            throw new IllegalArgumentException("Invalid search kind " + kind + " for VersionHistory.GetVersion");
        }
    }
}