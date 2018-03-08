package org.garret.perst;

import java.util.*;

/**
 * Interface of object spatial index.
 * Spatial index is used to allow fast selection of spatial objects belonging to the specified rectangle.
 * Spatial index is implemented using Guttman R-Tree with quadratic split algorithm.
 */
public interface SpatialIndexRn<T> extends IPersistent, IResource, ITable<T> { 
    /**
     * Find all objects located in the selected rectangle
     * @param r selected rectangle
     * @return array of objects which enveloping rectangle intersects with specified rectangle
     */
    public Object[] get(RectangleRn r);

    /**
     * Find all objects located in the selected rectangle
     * @param r selected rectangle
     * @return array list of objects which enveloping rectangle intersects with specified rectangle
     */
    public ArrayList<T> getList(RectangleRn r);
    
    /**
     * Put new object in the index. 
     * @param r enveloping rectangle for the object
     * @param obj object associated with this rectangle. Object can be not yet persistent, in this case
     * its forced to become persistent by assigning OID to it.
     */
    public void put(RectangleRn r, T obj);

    /**
     * Remove object with specified enveloping rectangle from the tree.
     * @param r enveloping rectangle for the object
     * @param obj object removed from the index
     * @exception StorageError (StorageError.KEY_NOT_FOUND) exception if there is no such key in the index
     */
    public void remove(RectangleRn r, T obj);

    /**
     * Get wrapping rectangle 
     * @return minimal rectangle containing all rectangles in the index, <code>null</code> if index is empty     
     */
    public RectangleRn getWrappingRectangle();
    
    /**
     * Get iterator through all members of the index
     * This iterator doesn't support remove() method.
     * It is not possible to modify spatial index during iteration.
     * @return iterator through all objects in the index
     */
    public Iterator<T> iterator();

    /**
     * Get entry iterator through all members of the index
     * This iterator doesn't support remove() method.
     * It is not possible to modify spatial index during iteration.
     * @return entry iterator which key specifies recrtangle and value - correspondent object
     */
    public IterableIterator<Map.Entry<RectangleRn,T>> entryIterator();

    /**
     * Get objects which rectangle intersects with specified rectangle
     * This iterator doesn't support remove() method.
     * It is not possible to modify spatial index during iteration.
     * @param r selected rectangle
     * @return iterator for objects which enveloping rectangle overlaps with specified rectangle
     */
    public IterableIterator<T> iterator(RectangleRn r); 

    /**
     * Get entry iterator through objects which rectangle intersects with specified rectangle
     * This iterator doesn't support remove() method.
     * It is not possible to modify spatial index during iteration.
     * @param r selected rectangle
     * @return entry iterator for objects which enveloping rectangle overlaps with specified rectangle
     */
    public IterableIterator<Map.Entry<RectangleRn,T>> entryIterator(RectangleRn r);

    /**
     * Get iterator through all neighbors of the specified point in the order of increasing distance 
     * from the specified point to the wrapper rectangle of the object
     * @param center coordinate of center point
     * @return iterator through all objects in the index in the order of increasing distance from the specified point
     */
    public IterableIterator<T> neighborIterator(PointRn center);
}
