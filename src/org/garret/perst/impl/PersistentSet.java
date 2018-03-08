package org.garret.perst.impl;
import  org.garret.perst.*;
import  java.util.*;

class JoinSetIterator<T> extends IterableIterator<T> implements PersistentIterator 
{ 
    private PersistentIterator i1;
    private PersistentIterator i2;
    private int currOid;
    private Storage storage;

    JoinSetIterator(Storage storage, Iterator<T> left, Iterator<T> right) { 
        this.storage = storage;
        i1 = (PersistentIterator)left;
        i2 = (PersistentIterator)right;
    }
    
    public boolean hasNext() { 
        if (currOid == 0) { 
            int oid1, oid2 = 0;
            while ((oid1 = i1.nextOid()) != 0) { 
                while (oid1 > oid2) { 
                    if ((oid2 = i2.nextOid()) == 0) {
                        return false;
                    }
                }
                if (oid1 == oid2) { 
                    currOid = oid1;
                    return true;
                }
            }
            return false;
        }
        return true;
    }
    
    public T next() { 
        if (!hasNext()) { 
            throw new NoSuchElementException();
        }
        return (T)storage.getObjectByOID(currOid);
    }
    
    public int nextOid() { 
        return hasNext() ? currOid : 0;
    }
    
    public void remove() {
        throw new UnsupportedOperationException();
    }
}           

class PersistentSet<T> extends Btree<T> implements IPersistentSet<T> 
{ 
    PersistentSet(boolean unique) { 
        type = ClassDescriptor.tpObject;
        this.unique = unique;
    }

    PersistentSet() {}
        
    public boolean isEmpty() { 
        return nElems == 0;
    }

    public boolean contains(Object o) {
        Key key = new Key(o);
        Iterator i = iterator(key, key, ASCENT_ORDER);
        return i.hasNext();
    }
    
    public <E> E[] toArray(E[] arr) { 
        return (E[])super.toArray((T[])arr);
    }

    public boolean add(T obj) { 
        return put(new Key(obj), obj);
    }

    public boolean remove(Object o) { 
        T obj = (T)o;
        return removeIfExists(new BtreeKey(checkKey(new Key(obj)), getStorage().getOid(obj)));
    }

    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof Set)) {
            return false;
        }
        Collection c = (Collection) o;
        if (c.size() != size()) {
            return false;
        }
        return containsAll(c);
    }

    public int hashCode() {
        int h = 0;
        Iterator i = iterator();
        while (i.hasNext()) {
            h += getStorage().getOid(i.next());
        }
        return h;
    }

    public IterableIterator<T> join(Iterator<T> with) { 
        return with == null ? (IterableIterator<T>)iterator() : new JoinSetIterator<T>(getStorage(), iterator(), with);
    }    
}
