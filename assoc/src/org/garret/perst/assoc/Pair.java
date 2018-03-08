package org.garret.perst.assoc;

/**
 * Name-value pair.
 * Used to get/set items attributes
 */
public class Pair 
{ 
    public final String name;
    public final Object value;
    
    /**
     * Pair constructor
     * @param name attribute name
     * @param value  attribute value
     */
    public Pair(String name, Object value) 
    { 
        this.name = name;
        this.value = value;
    }
}
