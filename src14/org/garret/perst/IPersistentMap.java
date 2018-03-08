package org.garret.perst;

import java.util.*;

/**
 * Interface of persistent map
 */
public interface IPersistentMap extends SortedMap, IPersistent, IResource, ITable
{
    /**
     * Get entry for the specified key. 
     * This method can be used to obtains both key and value.
     * It is needed when key is persistent object.
     * @param key searched key
     * @return entry associated with this key or null if there is no such key in the map
     */
    Entry getEntry(Object key);
} 