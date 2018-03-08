package org.garret.rdf;

/**
 * Predefined names of type and properties
 */
public class Symbols { 
    public static final String RDFS="http://www.w3.org/2000/01/rdf-schema#";
    public static final String RDF="http://www.w3.org/1999/02/22-rdf-syntax-ns#" ;
    public static final String NS="http://www.perst.org#";
    
    public static final String Metatype=NS+"Metaclass";
    public static final String Type=RDFS+"Class";
    public static final String Subtype=RDFS+"subClassOf";
    public static final String Thing=NS+"thing";
    public static final String Rectangle=NS+"rectangle";
    public static final String Point=NS+"point";
    public static final String Keyword=NS+"keyword";
    public static final String Uri=RDF+"about";
    public static final String Ref=RDF+"resource";
    public static final String Timestamp=NS+"timestamp";
}
