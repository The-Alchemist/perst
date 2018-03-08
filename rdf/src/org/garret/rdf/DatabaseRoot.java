package org.garret.rdf;

import java.util.*;
import org.garret.perst.*;

/**
 * Root class for Perst storage
 */
public class DatabaseRoot extends PersistentResource 
{
   /**
    * Root object in the graph
    */
    public VersionHistory rootObject;
   /**
    * Index used to access object by URI prefix
    */
    public Index          prefixUriIndex;
   /**
    * Index used to access object by URI suffix
    */
    public Index          suffixUriIndex;
   /**
    * Index used to search object by string property name:value pair
    */
    public Index          strPropIndex;
   /**
    * Index used to search object by numeric property name:value pair
    */
    public Index          numPropIndex;
   /**
    * Index used to search object by datetime property name:value pair
    */
    public Index          timePropIndex;
   /**
    * Index used to search object by reference property name:value pair
    */
    public Index          refPropIndex;
   /**
    * Index used to locate property definition by property name
    */
    public FieldIndex     propDefIndex;
   /**
    * Index used to perform spatial search locating overlapped rectangles
    */
    public Index          inverseIndex;
   /**
    * Inverse keywords index
    */
    public SpatialIndexR2 spatialIndex;
   /**
    * Set of the latest versions
    */
    public IPersistentSet latest;
   /**
    * Timestamp index
    */
    public FieldIndex     timeIndex;
   /**
    * Type of the types
    */
    public VersionHistory metatype;
}
