package org.garret.perst;

import java.lang.annotation.*;

/**
  * Annotation for marking indexable fields used by Database class to create table descriptors. 
  * Indices can be unique or allow duplicates.
  * If index is marked as unique and during transaction commit it is find out that there is already some other object
  * with this key, NotUniqueException will be thrown
  * Case insensitive attribute is meaningful only for string keys and if set cause ignoring case
  * of key values.
  * Thick index should be used for keys with small set of unique values.
  */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Indexable {
    /**
     * Index may not contain dupicates
     */
    boolean unique() default false;
    
    /**
     * Index is optimized to handle large number of duplicate key values
     */
    boolean thick() default false;

    /**
     * String index is case insensitive
     */
    boolean caseInsensitive() default false;

    /**
     * Index supports fast access to elements by position
     */
    boolean randomAccess() default false;
    
    /**
     * 3-gram index for fast regular expression matching
     */
    boolean regex() default false;

    /**
     * Index on autoincremented key field
     */
    boolean autoincrement() default false;
}