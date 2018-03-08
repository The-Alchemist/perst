package org.garret.perst;

import java.util.Iterator;

/**
 * Interface of selectable collection.
 * Selectable collections allows to selct its memebers using JSQL query
 */
public interface ITable { 
    /**
     * Select members of the collection using search predicate
     * This iterator doesn't support remove() method.
     * @param cls class of index members
     * @param predicate JSQL condition
     * @return iterator through members of the collection matching search condition
     */
    public Iterator select(Class cls, String predicate);

    /**
     * Get iterator through table records
     * This iterator doesn't support remove() method.
     * @return table iterator
     */
    public Iterator iterator();

    /**
     * Get numebr of records in the table
     * @return number of records in the table
     */
    public int size();
}