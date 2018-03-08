package org.garret.perst;

/**
 * Modifiers used by reflection API to mark indexable fields for Database class.
 * This modified should eb either explicitely specified by programmer in org.garret.perst.reflect.Field
 * constructor, either set in Javadoc comments using @modifier tag and preprocessed by SerGen utility.
 */
public interface Modifier
{ 
    /**
     * Normal index should be created for this field
     */
    public static final int Indexable = 1;

    /**
     * Index should ignore case of characters for this field (applicable only for string fields)
     */
    public static final int CaseInsensitive = 2;

    /**
     * Index should not allow duplicates
     */
    public static final int Unique = 4;

    /**
     * Field should be included full text index
     */
    public static final int FullTextIndexable = 8;

    /**
     * Array field has fixed size
     */
    public static final int FixedSize = 16;

    /**
     * Index providing fast access to element by position
     */
    public static final int RandomAccess = 32;

}
