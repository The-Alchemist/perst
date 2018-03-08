package org.garret.perst.impl;
import  org.garret.perst.*;
import java.util.*;

class HashSetImpl<T> extends PersistentCollection<T> implements IPersistentSet<T> { 
    IPersistentHash<T,T> map;

    HashSetImpl(StorageImpl storage) { 
        super(storage);
        map = storage.createHash();
    }

    HashSetImpl() {}

    public boolean isEmpty() { 
        return size() != 0;
    }

    public int size() { 
        return map.size();
    }

    public void clear() { 
        map.clear();
    }
    
    public boolean contains(Object o) {
        return map.containsKey(o);
    }
    
    public Object[] toArray() { 
        return map.values().toArray();
    }
  
    public <E> E[] toArray(E a[]) { 
        return map.values().toArray(a);
    }
    
    public Iterator<T> iterator() { 
        return map.values().iterator();
    }
    
    public boolean add(T obj) { 
        if (map.containsKey(obj)) { 
            return false;
        }
        map.put(obj, obj);
        return true;
    }

    public boolean remove(Object o) { 
        return map.remove(o) != null;
    }
    
    public void deallocate() { 
        map.deallocate();
        super.deallocate();
    }

    public IterableIterator<T> join(Iterator<T> with) { 
        return with == null ? (IterableIterator<T>)iterator() : new JoinSetIterator<T>(getStorage(), iterator(), with);
    }        
}
    
