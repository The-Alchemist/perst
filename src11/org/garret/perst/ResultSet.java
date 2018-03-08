package org.garret.perst;

import java.util.Hashtable;
import java.util.Enumeration;

/**
 * Class used to combine results of different searches
 */
public class ResultSet implements Collection
{
    /**
     * Construct empty result set
     */
    public ResultSet() { 
    }

    /**
     * Construct result set from specified array
     * @param result array with search results
     */
    public ResultSet(IPersistent[] result) {
        or(result);
    }

    /**
     * Construct result set from specified iterator
     * @param result result iterator
     */
    public ResultSet(Iterator result) {
        or(result);
    }

    /**
     * Intersect this result set with specified result (retain only objects present in both sets)
     * @param result search result which is intersected with this result set
     */
    public synchronized void add(IPersistent[] result) {
        Hashtable intersection = new Hashtable();
        for (int i = 0; i < result.length; i++) { 
            IPersistent obj = (IPersistent)result[i];
            Integer key = new Integer(obj.getOid());
            if (set.containsKey(key)) { 
                intersection.put(key, obj);
            }
        }
        set = intersection;
    }
    
    /**
     * Intersect this result set with specified result (retain only objects present in both sets)
     * @param result search result which is intersected with this result set
     */
    public synchronized void and(Iterator result) {
        Hashtable intersection = new Hashtable();
        while (result.hasNext()) { 
            IPersistent obj = (IPersistent)result.next();
            Integer key = new Integer(obj.getOid());
            if (set.containsKey(key)) { 
                intersection.put(key, obj);
            }
        }
        set = intersection;
    }
    
    /**
     * Merge this result set with specified result (skipping duplicates)
     * @param result search result which is intersected with this result set
     */
    public synchronized void or(IPersistent[] result) {
        for (int i = 0; i < result.length; i++) { 
            IPersistent obj = (IPersistent)result[i];
            Integer key = new Integer(obj.getOid());
            set.put(key, obj);
        }
    }
    
    /**
     * Merge this result set with specified result (skipping duplicates)
     * @param result search result which is intersected with this result set
     */
    public synchronized void or(Iterator result) {
        while (result.hasNext()) { 
            IPersistent obj = (IPersistent)result.next();
            Integer key = new Integer(obj.getOid());
            set.put(key, obj);
        }
    }
    
    /**
     * Filter result set, leaving only elements metching filter condition (for which Filter.fit returns true)
     * @param f applied filter
     * @return number of filtered elements 
     */
    public synchronized int filter(Filter f) { 
        Enumeration e = set.elements();
        int n = 0;
        while (e.hasMoreElements()) { 
            Object obj = e.nextElement();
            if (!f.fit(obj)) { 
                Integer key = new Integer(((IPersistent)obj).getOid());
                set.remove(key);
            } else { 
                n += 1;
            }
        }
        return n;
    }

    /**
     * Filter result set, leaving only elements belonging to the specified type
     * @param type type to which result set elements should belong
     * @return number of filtered elements 
     */
    public synchronized int filter(Class type) { 
        Enumeration e = set.elements();
        int n = 0;
        while (e.hasMoreElements()) { 
            Object obj = e.nextElement();
            if (!type.isInstance(obj)) { 
                Integer key = new Integer(((IPersistent)obj).getOid());
                set.remove(key);
            } else { 
                n += 1;
            }
        }
        return n;
    }


    public int size() { 
        return set.size();
    }
    
    public boolean isEmpty() { 
        return set.isEmpty();
    }

    public synchronized boolean contains(Object o) { 
        if (o instanceof IPersistent) { 
            IPersistent obj = (IPersistent)o;
            Integer key = new Integer(obj.getOid());
            return set.containsKey(key);
        }
        return false;
    }

    class ResultSetIterator extends Iterator { 
        public int nextOid() { 
            return ((IPersistent)next()).getOid();
        }

        public boolean hasNext() { 
            return e.hasMoreElements();
        }
        
        public Object next() { 
            return curr = e.nextElement();
        }
         
        public void remove() { 
            set.remove(new Integer(((IPersistent)curr).getOid()));
            curr = null;
        }        

        ResultSetIterator() { 
            e = set.elements();
        }

        private Object curr;
        private Enumeration e;
    }
             
    public Iterator iterator() { 
        return new ResultSetIterator();
    }

    public synchronized Object[] toArray() {
        Object[] arr = new IPersistent[set.size()];
        Enumeration e = set.elements();
        for (int i = 0; e.hasMoreElements(); i++) { 
            arr[i] = e.nextElement();
        }
        return arr;
    }
    
    public synchronized Object[] toArray(Object a[]) { 
        Object[] arr = a.length >= set.size() ? a : (Object[])new IPersistent[set.size()];
        Enumeration e = set.elements();
        for (int i = 0; e.hasMoreElements(); i++) { 
            arr[i] = e.nextElement();
        }
        return arr;
    }

    public synchronized boolean add(Object o) {
        if (o instanceof IPersistent) { 
            Integer key = new Integer(((IPersistent)o).getOid());
            return set.put(key, o) == null;
        }
        return false;
    }

    public synchronized boolean remove(Object o) { 
        if (o instanceof IPersistent) { 
            Integer key = new Integer(((IPersistent)o).getOid());
            return set.remove(key) != null;
        }
        return false;        
    }        

    public synchronized boolean containsAll(Collection c) {
        Iterator i = c.iterator();        
        while (i.hasNext()) {
            if (!contains(i.next())) { 
                return false;
            }
        }
        return true;
    }

    public synchronized boolean addAll(Collection c) {
        Iterator i = c.iterator();
        boolean added = false;
        while (i.hasNext()) {
            added |= add(i.next());
        }
        return added;
    }

    public synchronized boolean removeAll(Collection c) { 
        Iterator i = c.iterator();
        boolean removed = false;
        while (i.hasNext()) {
            removed |= remove(i.next());
        }
        return removed;
    }
        
    public synchronized boolean retainAll(Collection c) {
        Iterator i = c.iterator();
        Hashtable intersection = new Hashtable();
        while (i.hasNext()) {
            Object o = i.next();
            if (contains(o)) { 
                Integer key = new Integer(((IPersistent)o).getOid());
                intersection.put(key, o);
            }
        }
        int size = set.size();
        set = intersection;
        return size != intersection.size();
    }
   
    public synchronized void clear() {
        set.clear();
    }

    public synchronized boolean equals(Object o) {
        return o instanceof ResultSet && set.equals(((ResultSet)o).set);
    }

    public synchronized int hashCode() {
        return set.hashCode();
    }

    private Hashtable set = new Hashtable();
}