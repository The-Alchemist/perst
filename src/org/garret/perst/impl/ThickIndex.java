package org.garret.perst.impl;
import  org.garret.perst.*;

import java.util.*;

class ThickIndex<T> extends PersistentCollection<T> implements Index<T> { 
    Index<Object> index;
    int           nElems;

    static final int BTREE_THRESHOLD = 128;

    ThickIndex(StorageImpl db, Class keyType) { 
        super(db);
        index = db.<Object>createIndex(keyType, true);
    }
    
    ThickIndex() {}
    
    private final T getFromRelation(Object s) {
        if (s == null) { 
            return null;
        }
        if (s instanceof Relation) { 
            Relation r = (Relation)s;
            if (r.size() == 1) { 
                return (T)r.get(0);
            }
        }
        throw new StorageError(StorageError.KEY_NOT_UNIQUE);
    }

    public T get(Key key) {
        return getFromRelation(index.get(key));
    }
                  
    public T get(Object key) {
        return getFromRelation(index.get(key));
    }

    public ArrayList<T> getList(Key from, Key till) { 
        return extendList(index.getList(from, till));
    }

    public ArrayList<T> getList(Object from, Object till) { 
        return extendList(index.getList(from, till));
    }
   
    public Object[] get(Key from, Key till) {
        return extend(index.get(from, till));
    }
     
    public Object[] get(Object from, Object till) {
        return extend(index.get(from, till));
    }
     
    private ArrayList<T> extendList(ArrayList s) { 
        ArrayList<T> list = new ArrayList<T>();
        for (int i = 0, n = s.size(); i < n; i++) { 
            list.addAll((Collection<T>)s.get(i));
        }
        return list;
    }


    protected Object[] extend(Object[] s) { 
        ArrayList list = new ArrayList();
        for (int i = 0; i < s.length; i++) { 
            list.addAll((Collection)s[i]);
        }
        return list.toArray();
    }

    public T get(String key) {
        return get(new Key(key));
    }
                      
    public Object[] getPrefix(String prefix) { 
        return extend(index.getPrefix(prefix));
    }
    
    public ArrayList<T> getPrefixList(String prefix) { 
        return extendList(index.getPrefixList(prefix));
    }
    
    public Object[] prefixSearch(String word) { 
        return extend(index.prefixSearch(word));
    }
           
    public ArrayList<T> prefixSearchList(String word) { 
        return extendList(index.prefixSearchList(word));
    }
           
    public int size() { 
        return nElems;
    }
    
    public void clear() { 
        for (Object p : index) { 
            ((IPersistent)p).deallocate();
        }
        index.clear();
        nElems = 0;
        modify();
    }

    public Object[] toArray() { 
        return extend(index.toArray());
    }
        
    public <E> E[] toArray(E[] arr) { 
        ArrayList<E> list = new ArrayList<E>();
        for (Object c : index) { 
            list.addAll((Collection<E>)c);
        }
        return list.toArray(arr);
    }

    static class ExtendIterator<E> extends IterableIterator<E> implements PersistentIterator {  
        public boolean hasNext() { 
            return inner != null;
        }

        public E next() { 
            if (inner == null) { 
                throw new NoSuchElementException();
            }
            E obj = inner.next();
            if (!inner.hasNext()) {                 
                if (outer.hasNext()) {
                    inner = ((Iterable<E>)outer.next()).iterator();
                } else { 
                    inner = null;
                }
            }
            return obj;
        }

        public int nextOid() { 
            if (inner == null) { 
                return 0;
            }
            int oid = ((PersistentIterator)inner).nextOid();
            while (oid == 0) {                 
                if (outer.hasNext()) {
                    inner = ((Iterable<E>)outer.next()).iterator();
                    oid = ((PersistentIterator)inner).nextOid();
                } else { 
                    inner = null;
                    break;
                }
            }
            return oid;
        }

        public void remove() { 
            throw new UnsupportedOperationException();
        }

        ExtendIterator(IterableIterator<?> iterable) { 
            this(iterable.iterator());
        }

        ExtendIterator(Iterator<?> iterator) { 
            outer = iterator;
            if (iterator.hasNext()) { 
                inner = ((Iterable<E>)iterator.next()).iterator();
            }
        }

        private Iterator    outer;
        private Iterator<E> inner;
    }

    static class ExtendEntry<E> implements Map.Entry<Object,E> {
        public Object getKey() { 
            return key;
        }

        public E getValue() { 
            return value;
        }

        public E setValue(E value) { 
            throw new UnsupportedOperationException();
        }

        ExtendEntry(Object key, E value) {
            this.key = key;
            this.value = value;
        }

        private Object key;
        private E      value;
    }

    static class ExtendEntryIterator<E> extends IterableIterator<Map.Entry<Object,E>> {  
        public boolean hasNext() { 
            return inner != null;
        }

        public Map.Entry<Object,E> next() { 
            ExtendEntry<E> curr = new ExtendEntry<E>(key, inner.next());
            if (!inner.hasNext()) {                 
                if (outer.hasNext()) {
                    Map.Entry entry = (Map.Entry)outer.next();
                    key = entry.getKey();
                    inner = ((Iterable<E>)entry.getValue()).iterator();
                } else { 
                    inner = null;
                }
            }
            return curr;
        }

        public void remove() { 
            throw new UnsupportedOperationException();
        }

        ExtendEntryIterator(IterableIterator<?> iterator) { 
            outer = iterator;
            if (iterator.hasNext()) { 
                Map.Entry entry = (Map.Entry)iterator.next();
                key = entry.getKey();
                inner = ((Iterable<E>)entry.getValue()).iterator();
            }
        }

        private Iterator    outer;
        private Iterator<E> inner;
        private Object      key;
    }

    class ExtendEntryStartFromIterator extends ExtendEntryIterator<T> {  
        ExtendEntryStartFromIterator(int start, int order) { 
            super(entryIterator(null, null, order));
            int skip = (order == ASCENT_ORDER) ? start : nElems - start - 1;
            while (--skip >= 0 && hasNext()) {
                next();
            }
        }
    }            

    public Iterator<T> iterator() { 
        return new ExtendIterator<T>(index.iterator());
    }
    
    public IterableIterator<Map.Entry<Object,T>> entryIterator() { 
        return new ExtendEntryIterator<T>(index.entryIterator());
    }

    public IterableIterator<T> iterator(Key from, Key till, int order) { 
        return new ExtendIterator<T>(index.iterator(from, till, order));
    }
        
    public IterableIterator<T> iterator(Object from, Object till, int order) { 
        return new ExtendIterator<T>(index.iterator(from, till, order));
    }
        
    public IterableIterator<Map.Entry<Object,T>> entryIterator(Key from, Key till, int order) { 
        return new ExtendEntryIterator<T>(index.entryIterator(from, till, order));
    }

    public IterableIterator<Map.Entry<Object,T>> entryIterator(Object from, Object till, int order) { 
        return new ExtendEntryIterator<T>(index.entryIterator(from, till, order));
    }

    public IterableIterator<T> prefixIterator(String prefix) { 
        return prefixIterator(prefix, ASCENT_ORDER);
    }

    public IterableIterator<T> prefixIterator(String prefix, int order) { 
        return new ExtendIterator<T>(index.prefixIterator(prefix, order));
    }

    public Class getKeyType() { 
        return index.getKeyType();
    }

    public Class[] getKeyTypes() {
        return new Class[]{getKeyType()};
    }

    public boolean put(Key key, T obj) { 
        Object s = index.get(key);
        Storage storage = getStorage();
        int oid = storage.getOid(obj);
        if (oid == 0) { 
            oid = storage.makePersistent(obj);
        }
        if (s == null) { 
            Relation<T,ThickIndex> r = storage.<T,ThickIndex>createRelation(null);
            r.add(obj);
            index.put(key, r);
        } else if (s instanceof Relation) { 
            Relation rel = (Relation)s;
            if (rel.size() == BTREE_THRESHOLD) {
                IPersistentSet<T> ps = storage.<T>createBag();
                for (int i = 0; i < BTREE_THRESHOLD; i++) { 
                    ps.add((T)rel.get(i));
                }
                Assert.that(ps.add(obj));
                index.set(key, ps);
                rel.deallocate();
            } else { 
                int l = 0, n = rel.size(), r = n;
                while (l < r) { 
                    int m = (l + r) >>> 1;
                    if (storage.getOid(rel.getRaw(m)) <= oid) { 
                        l = m + 1;
                    } else { 
                        r = m;
                    }
                }
                rel.insert(r, obj);
            }
        } else { 
            Assert.that(((IPersistentSet<T>)s).add(obj));
        }
        nElems += 1;
        modify();
        return true;
    }

    public T set(Key key, T obj) {
        Object s = index.get(key);
        Storage storage = getStorage();
        int oid = storage.getOid(obj);
        if (oid == 0) { 
            oid = storage.makePersistent(obj);
        }
        if (s == null) { 
            Relation<T,ThickIndex> r = storage.<T,ThickIndex>createRelation(null);
            r.add(obj);
            index.put(key, r);
            nElems += 1;
            modify();
            return null;
        } else if (s instanceof Relation) { 
            Relation r = (Relation)s;
            if (r.size() == 1) {
                Object prev = r.get(0);
                r.set(0, obj);
                return (T)prev;
            } 
        }
        throw new StorageError(StorageError.KEY_NOT_UNIQUE);
    }

    public boolean unlink(Key key, T obj) {
        return removeIfExists(key, obj);
    }
    
    boolean removeIfExists(Key key, T obj) { 
        Object s = index.get(key);
        if (s instanceof Relation) { 
            Relation rel = (Relation)s;
            Storage storage = getStorage();
            int oid = storage.getOid(obj);
            int l = 0, n = rel.size(), r = n;
            while (l < r) { 
                int m = (l + r) >>> 1;
                if (storage.getOid(rel.getRaw(m)) < oid) { 
                    l = m + 1;
                } else { 
                    r = m;
                }
            }
            if (r < n && storage.getOid(rel.getRaw(r)) == oid) { 
                rel.remove(r);
                if (rel.size() == 0) { 
                    index.remove(key, rel);
                    rel.deallocate();
                }
                nElems -= 1;
                modify();
                return true;
            }
        } else if (s instanceof IPersistentSet) { 
            IPersistentSet ps = (IPersistentSet)s;
            if (ps.remove(obj)) { 
                if (ps.size() == 0) { 
                    index.remove(key, ps);
                    ps.deallocate();
                }                    
                nElems -= 1;
                modify();
                return true;
            }
        }
        return false;
    }

    public void remove(Key key, T obj) { 
        if (!removeIfExists(key, obj)) { 
            throw new StorageError(StorageError.KEY_NOT_FOUND);
        }
    }

    public T remove(Key key) {
        throw new StorageError(StorageError.KEY_NOT_UNIQUE);
    }

    public boolean put(Object key, T obj) {
        return put(Btree.getKeyFromObject(key), obj);
    }

    public T set(Object key, T obj) {
        return set(Btree.getKeyFromObject(key), obj);
    }

    public void remove(Object key, T obj) {
        remove(Btree.getKeyFromObject(key), obj);
    }

    public T remove(String key) {
        throw new StorageError(StorageError.KEY_NOT_UNIQUE);
    }

    public T removeKey(Object key) {
        throw new StorageError(StorageError.KEY_NOT_UNIQUE);
    }

    public void deallocate() {
        clear();
        index.deallocate();
        super.deallocate();
    }

    public int indexOf(Key key) { 
        PersistentIterator iterator = (PersistentIterator)iterator(null, key, DESCENT_ORDER);
        int i;
        for (i = -1; iterator.nextOid() != 0; i++);
        return i;
    }

    public T getAt(int i) {
        IterableIterator<Map.Entry<Object,T>> iterator;
        if (i < 0 || i >= nElems) {
            throw new IndexOutOfBoundsException("Position " + i + ", index size "  + nElems);
        }            
        if (i <= (nElems/2)) {
            iterator = entryIterator(null, null, ASCENT_ORDER);
            while (--i >= 0) { 
                iterator.next();
            }
        } else {
            iterator = entryIterator(null, null, DESCENT_ORDER);
            i -= nElems;
            while (++i < 0) { 
                iterator.next();
            }
        }
        return iterator.next().getValue();   
    }

    public IterableIterator<Map.Entry<Object,T>> entryIterator(int start, int order) {
        return new ExtendEntryStartFromIterator(start, order);
    }

    public boolean isUnique() {
        return false;
    }
}