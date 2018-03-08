package org.garret.rdf;

/**
 * Name:value pair used to specify property value
 */

public class NameVal {
   /** 
    * Name of the property
    */ 
    public String name;
    
   /** 
    * Value of the property (may be null or pattern)
    */
    public Object val;

   /** 
    * Constructor of name:valur pair
    * 
    * @param name name of the property
    * @param val value of the property
    */
    public NameVal(String name, Object val) { 
        this.name = name;
        this.val = val;
    }
}

