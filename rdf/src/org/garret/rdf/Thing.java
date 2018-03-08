package org.garret.rdf;

import java.util.*;
import java.io.*;
import org.garret.perst.*;

/** 
 * Version of the object
 */
public class Thing extends Persistent {
    /**
     * Object version history
     */
    public VersionHistory  vh;
    
   /**
    * Particular version of type of the object
    */
    public Thing           type;
    
   /**
    * Version creation timestamp
    */
    public Date            timestamp;
    
   /**
    * Property values
    */
    public PropVal[]       props;

   /**
    * Check if it is the latest version in version history
    * @return true if version is the last in version history
    */
    public boolean isLatest() {
        return vh.getLatest() == this;
    }

   /**
    * Get values of the property with given name
    * @param propName name of the proiperty
    */
    public Object[] get(String propName) {
        if (Symbols.Timestamp.equals(propName)) {
            return new Object[]{timestamp};
        }
        ArrayList list = new ArrayList();
        for (int i = 0; i < props.length; i++) {             
            PropVal prop = props[i];             
            if (prop.def.name.equals(propName)) {
                list.add(prop.val);
            }
        }
        return list.toArray();
    }
    
   /**
    * Check if object belongs to the partiular type
    * @param superType version history representing object type
    * @param kind search kind
    * @param timestamp timestamp used to locate version
    * @return true if type of the object is the same or is subtype of specified type
    */
    public boolean isInstanceOf(VersionHistory superType, SearchKind kind, Date timestamp) {
        return type.isSubTypeOf(superType, kind, timestamp);
    }
    
   /** This method is applicable only to objects represnting types and 
    * checks if this type is the same or subtype of specified type
    * @param superType version history representing object type
    * @param kind search kind
    * @param timestamp timestamp used to locate version
    * @return true if this type is the same or is subtype of specified type
    */
    public boolean isSubTypeOf(VersionHistory superType, SearchKind kind, Date timestamp) {
        if (vh == superType) { 
            return true;
        }
        Object[] subtypes = get(Symbols.Subtype);
        for (int i = 0; i < subtypes.length; i++) { 
            VersionHistory subtype = (VersionHistory)subtypes[i];
            if (kind == SearchKind.AllVersions) {
                for (int j = subtype.versions.size(); --j >= 0;) { 
                    Thing type = (Thing)subtype.versions.get(j);
                    if (type.isSubTypeOf(superType, kind, timestamp)) {
                        return true;
                    }
                }
            } else {
                Thing type = subtype.getVersion(kind, timestamp);
                if (type != null && type.isSubTypeOf(superType, kind, timestamp)) {
                    return true;
                }
            }
        }
        return false;
    }
}
