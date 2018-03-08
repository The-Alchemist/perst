package org.garret.rdf;

/** 
 * Class used to represent range of property values for search queries
 */
public class Range {
   /** 
    * Low boundary
    */ 
    public final Object from;

   /** 
    * Whether low boundary is inclusive or exclusive
    */ 
    public final boolean fromInclusive;

   /** 
    * High boundary
    */ 
    public final Object till;

    /** 
     * Whether high boundary is inclusive or exclusive
     */ 
    public final boolean tillInclusive;

    /** 
     * Range constructor 
     * 
     * @param from low boundary
     * @param fromInclusive is low boundary inclusive or exclusive
     * @param till high boundary
     * @param tillInclusive is high boundary inclusive or exclusive
     */
     public Range(Object from, boolean fromInclusive, Object till, boolean tillInclusive) {
         this.from = from;
         this.fromInclusive = fromInclusive;
         this.till = till;
         this.tillInclusive = tillInclusive;
     }
}