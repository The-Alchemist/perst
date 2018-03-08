package org.garret.perst.continuous;

import java.util.*;
import org.garret.perst.*;

class IndexFilter<T> extends PersistentCollection<T> implements GenericIndex<T> 
{
    IndexFilter(TableDescriptor.IndexDescriptor desc, IResource resource, VersionSelector selector)
    {
        this.index = desc.index;
        this.resource = resource;
        this.selector = selector;
        unique = desc.unique;
    }

    public Object[] toArray() {
        throw new AbstractMethodError();
    }
        
    public <E> E[] toArray(E[] arr) {
        throw new AbstractMethodError();
    }
    
    public boolean isUnique() { 
        return unique;
    }

    public T get(Key key) { 
        throw new AbstractMethodError();
    }

    public ArrayList<T> getList(Key from, Key till) { 
        throw new AbstractMethodError();
    }

    public Object[] get(Key from, Key till) { 
        throw new AbstractMethodError();
    }

    public T get(Object key) {
        throw new AbstractMethodError();
    }
    
    public Object[] get(Object from, Object till) {
        throw new AbstractMethodError();
    }
    
    public ArrayList<T> getList(Object from, Object till) {
        throw new AbstractMethodError();
    }
    
    public Object[] getPrefix(String prefix) {
        throw new AbstractMethodError();
    }
   
    public ArrayList<T> getPrefixList(String prefix) {
        throw new AbstractMethodError();
    }
    
    public Object[] prefixSearch(String word) {
        throw new AbstractMethodError();
    }
    
    public ArrayList<T> prefixSearchList(String word) {
        throw new AbstractMethodError();
    }
    
    public Iterator<T> iterator() {
        throw new AbstractMethodError();
    }

    public IterableIterator<Map.Entry<Object,T>> entryIterator() {
        throw new AbstractMethodError();
    }

    public IterableIterator<T> iterator(Key from, Key till, int order) 
    {
        resource.sharedLock();
        try { 
            return new IndexIterator<T>(index.iterator(from, till, order), resource, selector);
        } finally { 
            resource.unlock();
        } 
    }

    public IterableIterator<T> iterator(Object from, Object till, int order) {
        throw new AbstractMethodError();
    }

    public IterableIterator<Map.Entry<Object,T>> entryIterator(Key from, Key till, int order) {
        throw new AbstractMethodError();
    }

    public IterableIterator<Map.Entry<Object,T>> entryIterator(Object from, Object till, int order) {
        throw new AbstractMethodError();
    }

    public IterableIterator<T> prefixIterator(String prefix) 
    {
        resource.sharedLock();
        try { 
            return new IndexIterator<T>(index.prefixIterator(prefix), resource, selector);
        } finally { 
            resource.unlock();
        }
    }

    public int indexOf(Key key)
    {
        return index.indexOf(key);
    }

    public Class getKeyType() { 
        return index.getKeyType();
    }

    public Class[] getKeyTypes() { 
        return index.getKeyTypes();
    }

    public int size() {
        return index.size();
    }
    
    public void clear() { 
        index.clear();
    }

    public T getAt(int i) {
        throw new AbstractMethodError();
    }

    public IterableIterator<Map.Entry<Object,T>> entryIterator(int start, int order) { 
        throw new AbstractMethodError();
    }

    private Index<VersionHistorySegment> index;
    private IResource resource;
    private VersionSelector selector;
    private boolean unique;
}
