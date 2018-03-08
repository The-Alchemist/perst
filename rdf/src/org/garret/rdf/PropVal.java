package org.garret.rdf;

import org.garret.perst.*;


/**
 * Object property
 */
public class PropVal implements IValue {
   /**
    * Reference to property defintion (name of the property)    
    */
    public PropDef def;

    /**
     * Property value
     */
    public Object  val;
}    
