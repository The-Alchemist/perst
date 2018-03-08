package org.garret.perst.impl;
import  org.garret.perst.*;
import  java.util.*;

class PersistentHashImpl<K, V> extends PersistentResource implements IPersistentHash<K, V>
{
    static class HashPage extends Persistent 
    {
        Link items;
    
        HashPage(Storage db, int pageSize) 
        { 
            super(db);
            items = db.createLink(pageSize);
            items.setSize(pageSize);
        }
        
        HashPage() {}
        
        public void deallocate()
        {
            for (Object child : items) {
                if (child instanceof HashPage) {
                    ((HashPage)child).deallocate();
                } else {
                    CollisionItem next;
                    for (CollisionItem item = (CollisionItem)child; item != null; item = next) {
                        next = item.next;
                        item.deallocate();
                    }
                }
            }
            super.deallocate();
        }        
    }

    static class CollisionItem<K,V> extends Persistent implements Entry<K,V>
    { 
        K                  key;
        V                  obj;
        int                hashCode;
        CollisionItem<K,V> next;
         
        public K getKey() { 
            return key;
        }

        public V getValue() { 
            return obj;
        }

        public V setValue(V value) { 
            modify();
            V prevValue = obj;
            obj = value;
            return prevValue;
        }

        CollisionItem(K key, V obj, int hashCode) 
        {
            this.key = key;
            this.obj = obj;
            this.hashCode = hashCode;
        }
         
        CollisionItem() {}
    }    
             
    HashPage root;
    int      nElems;
    int      loadFactor;
    int      pageSize;
 
    transient volatile Set<Entry<K,V>> entrySet;
    transient volatile Set<K>          keySet;
    transient volatile Collection<V>   valuesCol;

    PersistentHashImpl(Storage storage, int pageSize, int loadFactor) { 
        super(storage);
        this.pageSize = pageSize;
        this.loadFactor = loadFactor;
    }

    PersistentHashImpl() {}

    public int size() {
        return nElems;
    }

    public boolean isEmpty() {
	return nElems == 0;
    }

    public boolean containsValue(Object value) {
	Iterator<Entry<K,V>> i = entrySet().iterator();
	if (value==null) {
	    while (i.hasNext()) {
		Entry<K,V> e = i.next();
		if (e.getValue()==null)
		    return true;
	    }
	} else {
	    while (i.hasNext()) {
		Entry<K,V> e = i.next();
		if (value.equals(e.getValue()))
		    return true;
	    }
	}
	return false;
    }

    public boolean containsKey(Object key) 
    {
        return getEntry(key) != null;
    }

    public V get(Object key) 
    {
        Entry<K,V> entry = getEntry(key);
        return entry != null ? entry.getValue() : null;           
    }

    private static final long UINT_MASK = 0xFFFFFFFFL;

    public Entry<K,V> getEntry(Object key) 
    {
        HashPage pg = root; 
        if (pg != null) {
            int divisor = 1;
            int hashCode = key.hashCode();
            while (true) {
                int h = (int)((hashCode & UINT_MASK) / divisor % pageSize);
                Object child = pg.items.get(h);
                if (child instanceof HashPage) {
                    pg = (HashPage)child; 
                    divisor *= pageSize;
                } else {
                    for (CollisionItem<K,V> item = (CollisionItem<K,V>)child; item != null; item = item.next) {
                        if (item.hashCode == hashCode && item.key.equals(key)) {
                            return item;
                        }
                    }
                    break;
                }
            }
        }
        return null;
    }
    
    public V put(K key, V value) 
    {
        int hashCode = key.hashCode();
        HashPage pg = root;
        if (pg == null) {
            pg = new HashPage(getStorage(), pageSize);
            int h = (int)((hashCode & UINT_MASK) % pageSize);
            pg.items.set(h, new CollisionItem<K,V>(key, value, hashCode));
            root = pg;
            nElems = 1;
            modify();
            return null;
        } else { 
            int divisor = 1;
            while (true)
            {
                int h = (int)((hashCode & UINT_MASK) / divisor % pageSize);
                Object child = pg.items.get(h);
                if (child instanceof HashPage) {
                    pg = (HashPage)child;
                    divisor *= pageSize;                        
                } else { 
                    CollisionItem<K,V> prev = null;
                    CollisionItem<K,V> last = null;
                    int collisionChainLength = 0;
                    for (CollisionItem<K,V> item = (CollisionItem<K,V>)child; item != null; item = item.next) {
                         if (item.hashCode == hashCode) {
                             if (item.key.equals(key)) {  
                                 V prevValue = item.obj;
                                 item.obj = value;
                                 item.modify();
                                 return prevValue;
                             }      
                             if (prev == null || prev.hashCode != hashCode) {
                                 collisionChainLength += 1;
                             }                           
                             prev = item; 
                         } else {      
                             collisionChainLength += 1;
                         }            
                         last = item;                
                    }
                    if (prev == null || prev.hashCode != hashCode) {
                        collisionChainLength += 1;
                    }
                    if (collisionChainLength > loadFactor) {
                        HashPage newPage = new HashPage(getStorage(), pageSize);
                        divisor *= pageSize;
                        CollisionItem<K,V> next;
                        for (CollisionItem<K,V> item = (CollisionItem<K,V>)child; item != null; item = next) {
                            next = item.next;
                            int hc = (int)((item.hashCode & UINT_MASK) / divisor % pageSize);                        
                            item.next = (CollisionItem<K,V>)newPage.items.get(hc);
                            newPage.items.set(hc, item);    
                            item.modify();
                        }
                        pg.items.set(h, newPage);
                        pg.modify();
                        pg = newPage;      
                    } else {
                        CollisionItem<K,V> newItem = new CollisionItem<K,V>(key, value, hashCode);
                        if (prev == null) {
                            prev = last;
                        }
                        if (prev != null) {
                            newItem.next = prev.next;
                            prev.next = newItem;
                            prev.modify();
                        } else {
                            pg.items.set(h, newItem);
                            pg.modify();
                        }
                        nElems += 1;
                        modify();
                        return null;
                    }                            
                }
            }
        }
    }

    public V remove(Object key) 
    {
        HashPage pg = root; 
        if (pg != null) {
            int divisor = 1;
            int hashCode = key.hashCode();
            while (true) {
                int h = (int)((hashCode & UINT_MASK) / divisor % pageSize);
                Object child = pg.items.get(h);
                if (child instanceof HashPage) {
                    pg = (HashPage)child; 
                    divisor *= pageSize;
                } else {
                    CollisionItem<K,V> prev = null;
                    for (CollisionItem<K,V> item = (CollisionItem<K,V>)child; item != null; item = item.next) {
                        if (item.hashCode == hashCode && item.key.equals(key)) {
                            V obj = item.obj;
                            if (prev != null) {
                                prev.next = item.next;
                                prev.modify();
                            } else {
                                pg.items.set(h, item.next);
                                pg.modify();
                            }
                            nElems -= 1;
                            modify();
                            return obj;
                        }
                        prev = item;
                    }
                    break;
                }
            }
        }
        return null;
    }

    public void putAll(Map<? extends K, ? extends V> t) {
	Iterator<? extends Entry<? extends K, ? extends V>> i = t.entrySet().iterator();
	while (i.hasNext()) {
	    Entry<? extends K, ? extends V> e = i.next();
	    put(e.getKey(), e.getValue());
	}
    }

    public void clear() 
    {
        if (root != null) {
            root.deallocate();
            root = null;
            nElems = 0;
            modify();
        }
    }

    public Set<K> keySet() {
	if (keySet == null) {
	    keySet = new AbstractSet<K>() {
		public Iterator<K> iterator() {
		    return new Iterator<K>() {
			private Iterator<Entry<K,V>> i = entrySet().iterator();

			public boolean hasNext() {
			    return i.hasNext();
			}

			public K next() {
			    return i.next().getKey();
			}

			public void remove() {
			    i.remove();
			}
                    };
		}

		public int size() {
		    return PersistentHashImpl.this.size();
		}

		public boolean contains(Object k) {
		    return PersistentHashImpl.this.containsKey(k);
		}
	    };
	}
	return keySet;
    }

    public Collection<V> values() {
	if (valuesCol == null) {
	    valuesCol = new AbstractCollection<V>() {
		public Iterator<V> iterator() {
		    return new Iterator<V>() {
			private Iterator<Entry<K,V>> i = entrySet().iterator();

			public boolean hasNext() {
			    return i.hasNext();
			}

			public V next() {
			    return i.next().getValue();
			}

			public void remove() {
			    i.remove();
			}
                    };
                }

		public int size() {
		    return PersistentHashImpl.this.size();
		}

		public boolean contains(Object v) {
		    return PersistentHashImpl.this.containsValue(v);
		}
	    };
	}
	return valuesCol;
    }

    protected Iterator<Entry<K,V>> entryIterator() {
        return new EntryIterator();
    }

    static class StackElem
    {
        HashPage page;
        int      pos;

        StackElem(HashPage page, int pos) {
            this.page = page;
            this.pos = pos;
        }
    }

    class EntryIterator implements Iterator<Entry<K,V>> {          
        CollisionItem<K,V> currItem;
        CollisionItem<K,V> nextItem;
        Stack<StackElem> stack = new Stack<StackElem>();

        EntryIterator() 
        {
            HashPage pg = root;
            
            if (pg != null) {
                int start = 0;
                int sp = 0;
              DepthFirst:
                while (true) { 
                    for (int i = start, n = pg.items.size(); i < n; i++) { 
                        Object child = pg.items.get(i);
                        if (child != null) {
                            stack.push(new StackElem(pg, i));
                            if (child instanceof HashPage) {
                                pg = (HashPage)child;
                                start = 0;
                                continue DepthFirst;
                            } else {
                                nextItem = (CollisionItem<K,V>)child;
                                return;
                            }
                        }
                    }
                    if (stack.isEmpty()) {
                        break;
                    } else { 
                        StackElem top = stack.pop();
                        pg = top.page;
                        start = top.pos + 1;
                    } 
                }
            } 
        }

        public boolean hasNext() {
            return nextItem != null;
        }
                    
        public Entry<K,V> next() {
            if (nextItem == null) {
                throw new NoSuchElementException(); 
            }
            currItem = nextItem;       
            if ((nextItem = nextItem.next) == null) {
                do {
                    StackElem top = stack.pop();
                    HashPage pg = top.page;
                    int start = top.pos + 1;                           
                    
                  DepthFirst:
                    while (true) {
                        for (int i = start, n = pg.items.size(); i < n; i++) { 
                            Object child = pg.items.get(i);
                            if (child != null) {
                                stack.push(new StackElem(pg, i));
                                if (child instanceof HashPage) {
                                    pg = (HashPage)child;
                                    start = 0;
                                    continue DepthFirst;
                                } else {
                                    nextItem = (CollisionItem<K,V>)child;
                                    return currItem;
                                }
                            }
                        }
                        break;
                    } 
                } while (!stack.isEmpty());
            }            
            return currItem;
        }

        public void remove() {
            if (currItem == null) {
                throw new NoSuchElementException(); 
            }
            PersistentHashImpl.this.remove(currItem.key);
        }
    }

    public Set<Entry<K,V>> entrySet() {
	if (entrySet == null) {
	    entrySet = new AbstractSet<Entry<K,V>>() {
		public Iterator<Entry<K,V>> iterator() {
		    return entryIterator();
		}

		public int size() {
		    return PersistentHashImpl.this.size();
		}

                public boolean remove(Object o) {
                    if (!(o instanceof Map.Entry)) {
                        return false;
                    }
                    Map.Entry<K,V> entry = (Map.Entry<K,V>) o;
                    K key = entry.getKey();
                    V value = entry.getValue();
                    if (value != null) { 
                        V v = PersistentHashImpl.this.get(key);
                        if (value.equals(v)) {
                            PersistentHashImpl.this.remove(key);
                            return true;
                        }
                    } else {
                        if (PersistentHashImpl.this.containsKey(key)
                            && PersistentHashImpl.this.get(key) == null)
                        {
                            PersistentHashImpl.this.remove(key);
                            return true;
                        }
                    }
                    return false;
                }
                
		public boolean contains(Object k) {
                    Entry<K,V> e = (Entry<K,V>)k;
                    if (e.getValue() != null) { 
                        return e.getValue().equals(PersistentHashImpl.this.get(e.getKey()));
                    } else {
                        return PersistentHashImpl.this.containsKey(e.getKey()) 
                            && PersistentHashImpl.this.get(e.getKey()) == null;
                    }
		}
	    };
	}
	return entrySet;
    }   

     
    public boolean equals(Object o) {
	if (o == this) {
	    return true;
        }
	if (!(o instanceof Map)) {
	    return false;
        }
	Map<K,V> t = (Map<K,V>) o;
	if (t.size() != size()) {
	    return false;
        }

        try {
            Iterator<Entry<K,V>> i = entrySet().iterator();
            while (i.hasNext()) {
                Entry<K,V> e = i.next();
		K key = e.getKey();
                V value = e.getValue();
                if (value == null) {
                    if (!(t.get(key)==null && t.containsKey(key))) {
                        return false;
                    }
                } else {
                    if (!value.equals(t.get(key))) {
                        return false;
                    }
                }
            }
        } catch(ClassCastException unused) {
            return false;
        } catch(NullPointerException unused) {
            return false;
        }

	return true;
    }

    public int hashCode() {
	int h = 0;
	Iterator<Entry<K,V>> i = entrySet().iterator();
	while (i.hasNext()) {
	    h += i.next().hashCode();
        }
	return h;
    }

    public String toString() {
	StringBuffer buf = new StringBuffer();
	buf.append("{");

	Iterator<Entry<K,V>> i = entrySet().iterator();
        boolean hasNext = i.hasNext();
        while (hasNext) {
	    Entry<K,V> e = i.next();
	    K key = e.getKey();
            V value = e.getValue();
	    if (key == this) {
		buf.append("(this Hash)");
            } else {
		buf.append(key);
            }
	    buf.append("=");
	    if (value == this) {
		buf.append("(this Hash)");
            } else {
		buf.append(value);
            }
            hasNext = i.hasNext();
            if (hasNext) {
                buf.append(", ");
            }
        }

	buf.append("}");
	return buf.toString();
    }

    public Iterator<V> select(Class cls, String predicate) { 
        Query<V> query = new QueryImpl<V>(getStorage());
        return query.select(cls, values().iterator(), predicate);
    }
} 
