package org.garret.perst.impl;
import  org.garret.perst.*;
import  java.util.*;

class PersistentMapImpl extends PersistentResource implements IPersistentMap
{
    IPersistent index;
    Object      keys;
    Link        values;
    int         type;

    transient volatile Set        entrySet;
    transient volatile Set        keySet;
    transient volatile Collection valuesCol;

    static final int BTREE_TRESHOLD = 128;

    PersistentMapImpl(Storage storage, Class keyType, int initialSize) { 
        super(storage);
        type = getTypeCode(keyType);
        keys = new Comparable[initialSize];
        values = storage.createLink(initialSize);
    }

    PersistentMapImpl() {}

    static class PersistentMapEntry extends Persistent implements Entry { 
        Comparable  key;
        IPersistent value;

        public Object getKey() { 
            return key;
        }

        public Object getValue() { 
            return value;
        }

        public Object setValue(Object value) { 
            modify();
            IPersistent prevValue = this.value;
            this.value = (IPersistent)value;
            return prevValue;
        }

        PersistentMapEntry(Comparable key, IPersistent value) { 
            this.key = key;
            this.value = value;
        }
        PersistentMapEntry() {}
    }

    static class PersistentMapComparator extends PersistentComparator { 
        public int compareMembers(IPersistent m1, IPersistent m2) {
            return ((PersistentMapEntry)m1).key.compareTo(((PersistentMapEntry)m2).key);
        }

        public int compareMemberWithKey(IPersistent mbr, Object key) {
            return ((PersistentMapEntry)mbr).key.compareTo(key);
        }
    }

    protected int getTypeCode(Class c) { 
        if (c.equals(byte.class) || c.equals(Byte.class)) { 
            return ClassDescriptor.tpByte;
        } else if (c.equals(short.class) || c.equals(Short.class)) {
            return ClassDescriptor.tpShort;
        } else if (c.equals(char.class) || c.equals(Character.class)) {
            return ClassDescriptor.tpChar;
        } else if (c.equals(int.class) || c.equals(Integer.class)) {
            return ClassDescriptor.tpInt;
        } else if (c.equals(long.class) || c.equals(Long.class)) {
            return ClassDescriptor.tpLong;
        } else if (c.equals(float.class) || c.equals(Float.class)) {
            return ClassDescriptor.tpFloat;
        } else if (c.equals(double.class) || c.equals(Double.class)) {
            return ClassDescriptor.tpDouble;
        } else if (c.equals(String.class)) {
            return ClassDescriptor.tpString;
        } else if (c.equals(boolean.class) || c.equals(Boolean.class)) {
            return ClassDescriptor.tpBoolean;
        } else if (c.equals(java.util.Date.class)) {
            return ClassDescriptor.tpDate;
        } else if (Comparable.class.isAssignableFrom(c)) {
            return ClassDescriptor.tpRaw;
        } else { 
            throw new StorageError(StorageError.UNSUPPORTED_TYPE, c);
        }
    }

    public Iterator iterator() { 
        return values().iterator();
    }

    public int size() {
        return index != null ? ((ITable)index).size() : values.size();
    }

    public boolean isEmpty() {
	return size() == 0;
    }

    public boolean containsValue(Object value) {
	Iterator i = entrySet().iterator();
	if (value==null) {
	    while (i.hasNext()) {
		Entry e = (Entry)i.next();
		if (e.getValue()==null)
		    return true;
	    }
	} else {
	    while (i.hasNext()) {
		Entry e = (Entry)i.next();
		if (value.equals(e.getValue()))
		    return true;
	    }
	}
	return false;
    }

    private int binarySearch(Object key) {
        Comparable[] keys = (Comparable[])this.keys;
        int l = 0, r = values.size();
        while (l < r) {
            int i = (l + r) >> 1;
            if (keys[i].compareTo(key) < 0) { 
                l = i+1;
            } else { 
                r = i;
            }
        }
        return r;
    }

    public boolean containsKey(Object key) {
        if (index != null) { 
            if (type == ClassDescriptor.tpRaw) { 
                return ((SortedCollection)index).containsKey(key);
            } else { 
                Key k = generateKey(key);
                return ((Index)index).entryIterator(k, k, Index.ASCENT_ORDER).hasNext();
            } 
        } else {
            int i = binarySearch(key);
            return i < values.size() && ((Comparable[])keys)[i].equals(key);
        }
    }

    public Entry getEntry(Object key) {
        if (index != null) { 
            if (type == ClassDescriptor.tpRaw) { 
                return (Entry)((SortedCollection)index).get(key);
            } else { 
                IPersistent value = ((Index)index).get(generateKey(key));
                return value != null ? new PersistentMapEntry((Comparable)key, value) : null;
            }
        } else {
            int i = binarySearch(key);
            if (i < values.size() && ((Comparable[])keys)[i].equals(key)) {
                IPersistent value = values.get(i);
                return value != null ? new PersistentMapEntry((Comparable)key, value) : null;                
            }
            return null;
        }
    }
        
    public Object get(Object key) {
        if (index != null) { 
            if (type == ClassDescriptor.tpRaw) { 
                PersistentMapEntry entry = (PersistentMapEntry)((SortedCollection)index).get(key);
                return (entry != null) ? entry.value : null;
            } else { 
                return ((Index)index).get(generateKey(key));
            }
        } else {
            int i = binarySearch(key);
            if (i < values.size() && ((Comparable[])keys)[i].equals(key)) {
                return values.get(i);
            }
            return null;
        }
    }

    public Object put(Object key, Object value) {
        Object prev = null;
        if (index == null) { 
            int size = values.size();
            int i = binarySearch(key);
            if (i < size && key.equals(((Comparable[])keys)[i])) {
                prev = values.get(i);
                values.set(i, (IPersistent)value);
            } else {
                if (size == BTREE_TRESHOLD) {
                    Comparable[] keys = (Comparable[])this.keys;
                    if (type == ClassDescriptor.tpRaw) { 
                        SortedCollection col = getStorage().createSortedCollection(new PersistentMapComparator(), true);
                        index = col;
                        for (i = 0; i < size; i++) { 
                            col.add(new PersistentMapEntry(keys[i], values.get(i)));
                        }                
                        prev = insertInSortedCollection(key, value);
                    } else { 
                        Index idx = getStorage().createIndex(Btree.mapKeyType(type), true);
                        index = idx;
                        for (i = 0; i < size; i++) { 
                            idx.set(generateKey(keys[i]), values.get(i));
                        }                
                        prev = idx.set(generateKey(key), (IPersistent)value);
                    }
                    this.keys = null;
                    this.values = null;                
                    modify();
                } else {
                    Comparable[] oldKeys = (Comparable[])keys;
                    if (size >= oldKeys.length) { 
                        Comparable[] newKeys = new Comparable[size+1 > oldKeys.length*2 ? size+1 : oldKeys.length*2];
                        System.arraycopy(oldKeys, 0, newKeys, 0, i);                
                        System.arraycopy(oldKeys, i, newKeys, i+1, size-i);
                        keys = newKeys;
                        newKeys[i] = (Comparable)key;
                    } else {
                        System.arraycopy(oldKeys, i, oldKeys, i+1, size-i);
                        oldKeys[i] = (Comparable)key;
                    }
                    values.insert(i, (IPersistent)value);
                }
            }
        } else { 
            if (type == ClassDescriptor.tpRaw) {               
                prev = insertInSortedCollection(key, value);
            } else { 
                prev = ((Index)index).set(generateKey(key), (IPersistent)value);
            }
        }
        return prev;
    }

    private IPersistent insertInSortedCollection(Object key, Object value) {
        IPersistent obj = (IPersistent)value;
        SortedCollection col = (SortedCollection)index;
        PersistentMapEntry entry = (PersistentMapEntry)col.get(key);
        IPersistent prev = null;
        getStorage().makePersistent(obj);
        if (entry == null) { 
            col.add(new PersistentMapEntry((Comparable)key, obj));
        } else {
            prev = (IPersistent)entry.setValue(value);
        }
        return prev;
    }

    public Object remove(Object key) {
        if (index == null) { 
            int size = values.size();
            int i = binarySearch(key);
            if (i < size && ((Comparable[])keys)[i].equals(key)) {
                System.arraycopy(keys, i+1, keys, i, size-i-1);
                ((Comparable[])keys)[size-1] = null;
                Object prev = values.get(i);
                values.remove(i);
                return prev;
            }
            return null;
        } else {
            if (type == ClassDescriptor.tpRaw) {               
                SortedCollection col = (SortedCollection)index;
                PersistentMapEntry entry = (PersistentMapEntry)col.get(key);
                if (entry == null) { 
                    return null;
                }
                col.remove(entry);
                return entry.value;
            } else { 
                try { 
                    return ((Index)index).remove(generateKey(key));
                } catch (StorageError x) { 
                    if (x.getErrorCode() == StorageError.KEY_NOT_FOUND) { 
                        return null;
                    }
                    throw x;
                }
            }
        }
    }

    public void putAll(Map t) {
	Iterator i = t.entrySet().iterator();
	while (i.hasNext()) {
	    Entry e = (Entry)i.next();
	    put(e.getKey(), (IPersistent)e.getValue());
	}
    }

    public void clear() {
        if (index != null) { 
            if (type == ClassDescriptor.tpRaw) {               
                ((SortedCollection)index).clear();
            } else { 
                ((Index)index).clear();
            }
        } else {
            values.clear();
            keys = new Comparable[((Comparable[])keys).length];
        }
    }

    public Set keySet() {
	if (keySet == null) {
	    keySet = new AbstractSet() {
		public Iterator iterator() {
		    return new Iterator() {
			private Iterator i = entrySet().iterator();

			public boolean hasNext() {
			    return i.hasNext();
			}

			public Object next() {
			    return ((Entry)i.next()).getKey();
			}

			public void remove() {
			    i.remove();
			}
                    };
		}

		public int size() {
		    return PersistentMapImpl.this.size();
		}

		public boolean contains(Object k) {
		    return PersistentMapImpl.this.containsKey(k);
		}
	    };
	}
	return keySet;
    }

    public Collection values() {
	if (valuesCol == null) {
	    valuesCol = new AbstractCollection() {
		public Iterator iterator() {
		    return new Iterator() {
			private Iterator i = entrySet().iterator();

			public boolean hasNext() {
			    return i.hasNext();
			}

			public Object next() {
			    return ((Entry)i.next()).getValue();
			}

			public void remove() {
			    i.remove();
			}
                    };
                }

		public int size() {
		    return PersistentMapImpl.this.size();
		}

		public boolean contains(Object v) {
		    return PersistentMapImpl.this.containsValue(v);
		}
	    };
	}
	return valuesCol;
    }

    protected Iterator entryIterator() {
        if (index != null) { 
            if (type == ClassDescriptor.tpRaw) {               
                return ((SortedCollection)index).iterator();
            } else { 
                return ((Index)index).entryIterator();
            }
        } else {
            return new Iterator() {                     
                private int i = -1;

                public boolean hasNext() {
                    return i+1 < values.size();
                }

                public Object next() {
                    if (!hasNext()) { 
                        throw new NoSuchElementException(); 
                    }
                    i += 1;
                    return new Entry() {
                        public Object getKey() { 
                            return (((Comparable[])keys)[i]);
                        }
                        public Object getValue() { 
                            return values.get(i);
                        }
                        public Object setValue(Object value) {
                            throw new UnsupportedOperationException("Entry.Map.setValue");
                        }
                    };  
                }

                public void remove() {
                    if (i < 0) {
                        throw new IllegalStateException();
                    }
                    int size = values.size();
                    System.arraycopy(keys, i+1, keys, i, size-i-1);
                    ((Comparable[])keys)[size-1] = null;
                    values.remove(i);
                    i -= 1;
                }
            };
        }
    }

    public Set entrySet() {
	if (entrySet == null) {
	    entrySet = new AbstractSet() {
		public Iterator iterator() {
		    return entryIterator();
		}

		public int size() {
		    return PersistentMapImpl.this.size();
		}

                public boolean remove(Object o) {
                    if (!(o instanceof Map.Entry)) {
                        return false;
                    }
                    Map.Entry entry = (Map.Entry) o;
                    Object key = entry.getKey();
                    Object value = entry.getValue();
                    if (value != null) { 
                        Object v = PersistentMapImpl.this.get(key);
                        if (value.equals(v)) {
                            PersistentMapImpl.this.remove(key);
                            return true;
                        }
                    } else {
                        if (PersistentMapImpl.this.containsKey(key)
                            && PersistentMapImpl.this.get(key) == null)
                        {
                            PersistentMapImpl.this.remove(key);
                            return true;
                        }
                    }
                    return false;
                }
                
		public boolean contains(Object k) {
                    Entry e = (Entry)k;
                    if (e.getValue() != null) { 
                        return e.getValue().equals(PersistentMapImpl.this.get(e.getKey()));
                    } else {
                        return PersistentMapImpl.this.containsKey(e.getKey()) 
                            && PersistentMapImpl.this.get(e.getKey()) == null;
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
	Map t = (Map) o;
	if (t.size() != size()) {
	    return false;
        }

        try {
            Iterator i = entrySet().iterator();
            while (i.hasNext()) {
                Entry e = (Entry)i.next();
		Object key = e.getKey();
                Object value = e.getValue();
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
	Iterator i = entrySet().iterator();
	while (i.hasNext()) {
	    h += i.next().hashCode();
        }
	return h;
    }

    public String toString() {
	StringBuffer buf = new StringBuffer();
	buf.append("{");

	Iterator i = entrySet().iterator();
        boolean hasNext = i.hasNext();
        while (hasNext) {
	    Entry e = (Entry)i.next();
	    Object key = e.getKey();
            Object value = e.getValue();
	    if (key == this) {
		buf.append("(this Map)");
            } else {
		buf.append(key);
            }
	    buf.append("=");
	    if (value == this) {
		buf.append("(this Map)");
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

    final Key generateKey(Object key) {
        return generateKey(key, true);
    }

    final Key generateKey(Object key, boolean inclusive) {
        if (key instanceof Integer) { 
            return new Key(((Integer)key).intValue(), inclusive);
        } else if (key instanceof Byte) { 
            return new Key(((Byte)key).byteValue(), inclusive);
        } else if (key instanceof Character) { 
            return new Key(((Character)key).charValue(), inclusive);
        } else if (key instanceof Short) { 
            return new Key(((Short)key).shortValue(), inclusive);
        } else if (key instanceof Long) { 
            return new Key(((Long)key).longValue(), inclusive);
        } else if (key instanceof Float) { 
            return new Key(((Float)key).floatValue(), inclusive);
        } else if (key instanceof Double) { 
            return new Key(((Double)key).doubleValue(), inclusive);
        } else if (key instanceof String) { 
            return new Key((String)key, inclusive);
        } else if (key instanceof java.util.Date) { 
            return new Key((java.util.Date)key, inclusive);
        } else { 
            throw new StorageError(StorageError.UNSUPPORTED_INDEX_TYPE, key.getClass());
        }
    }

    public Comparator comparator() {
        return null;
    }

    public SortedMap subMap(Object from, Object to) {
        if (((Comparable)from).compareTo(to) > 0) {
            throw new IllegalArgumentException("from > to");
        }
        return new SubMap(from, to);
    }

    public SortedMap headMap(Object to) {
        return new SubMap(null, to);
    }

    public SortedMap tailMap(Object from) {
        return new SubMap(from, null);
    }

    private class SubMap extends AbstractMap implements SortedMap {
        private Key fromKey;
        private Key toKey;
        private Object from;
        private Object to;
        volatile Set   entrySet;

        SubMap(Object from, Object to) {
            this.from = from;
            this.to = to;
            this.fromKey = from != null ? generateKey(from, true) : null;
            this.toKey = to != null ? generateKey(to, false) : null;
        }

        public boolean isEmpty() {
            return entrySet().isEmpty();
        }

        public boolean containsKey(Object key) {
            return inRange(key) && PersistentMapImpl.this.containsKey(key);
        }

        public Object get(Object key) {
            if (!inRange(key)) {
                return null;
            }
            return PersistentMapImpl.this.get(key);
        }

        public Object put(Object key, Object value) {
            if (!inRange(key)) {
                throw new IllegalArgumentException("key out of range");
            }
            return PersistentMapImpl.this.put(key, value);
        }

        public Comparator comparator() {
            return null;
        }

        public Object firstKey() {
            return ((Entry)entryIterator(Index.ASCENT_ORDER).next()).getKey();
        }

        public Object lastKey() {
            return ((Entry)entryIterator(Index.DESCENT_ORDER).next()).getKey();
        }

        protected Iterator entryIterator(final int order) {
            if (index != null) { 
                if (type == ClassDescriptor.tpRaw) {               
                    if (order == Index.ASCENT_ORDER) { 
                        return ((SortedCollection)index).iterator(fromKey, toKey);
                    } else { 
                        final IPersistent[] entries = ((SortedCollection)index).get(fromKey, toKey);
                        return new Iterator() { 
                            private int i = entries.length;

                            public boolean hasNext() {
                                return i > 0;
                            }
                            public Object next() {
                                if (!hasNext()) { 
                                    throw new NoSuchElementException(); 
                                }
                                return entries[--i];
                            }
                            
                            public void remove() {
                                if (i < entries.length || entries[i] == null) {
                                    throw new IllegalStateException();
                                }
                                ((SortedCollection)index).remove(entries[i]);
                                entries[i] = null;
                            }
                        };
                    }        
                } else { 
                    return ((Index)index).entryIterator(fromKey, toKey, order);
                }
            } else {
                if (order == Index.ASCENT_ORDER) { 
                    final int beg = (from != null ? binarySearch(from) : 0) - 1;
                    final int end = values.size();
                    
                    return new Iterator() {                     
                        private int i = beg;
                            
                        public boolean hasNext() {
                            return i+1 < end && (to == null || ((Comparable[])keys)[i+1].compareTo(to) < 0);
                        }
                            
                        public Object next() {
                            if (!hasNext()) { 
                                throw new NoSuchElementException(); 
                            }
                            i += 1;
                            return new Entry() {
                                public Object getKey() { 
                                    return ((Comparable[])keys)[i];
                                }
                                public Object getValue() { 
                                    return values.get(i);
                                }
                                public Object setValue(Object value) {
                                    throw new UnsupportedOperationException("Entry.Map.setValue");
                                }
                            };  
                        }
                            
                        public void remove() {
                            if (i < 0) {
                                throw new IllegalStateException();
                            }
                            int size = values.size();
                            System.arraycopy(keys, i+1, keys, i, size-i-1);
                            ((Comparable[])keys)[size-1] = null;
                            values.remove(i);
                            i -= 1;
                        }
                    };
                } else {
                    final int beg = to != null ? binarySearch(to) : values.size();
                    
                    return new Iterator() {                     
                        private int i = beg;
                            
                        public boolean hasNext() {
                            return i > 0 && (from == null || ((Comparable[])keys)[i-1].compareTo(from) >= 0);
                        }
                            
                        public Object next() {
                            if (!hasNext()) { 
                                throw new NoSuchElementException(); 
                            }
                            i -= 1;
                            return new Entry() {
                                public Object getKey() { 
                                    return ((Comparable[])keys)[i];
                                }
                                public Object getValue() { 
                                    return values.get(i);
                                }
                                public Object setValue(Object value) {
                                    throw new UnsupportedOperationException("Entry.Map.setValue");
                                }
                            };  
                        }
                            
                        public void remove() {
                            if (i < 0) {
                                throw new IllegalStateException();
                            }
                            int size = values.size();
                            System.arraycopy(keys, i+1, keys, i, size-i-1);
                            ((Comparable[])keys)[size-1] = null;
                            values.remove(i);
                        }
                   };
                }
            }
        }

        public Set entrySet() {
            if (entrySet == null) {
                entrySet = new AbstractSet() {
                    public Iterator iterator() {
                        return entryIterator(Index.ASCENT_ORDER);
                    }
                    
                    public int size() {
                        Iterator i = iterator();
                        int n;
                        for (n = 0; i.hasNext(); i.next()) { 
                            n += 1;
                        }
                        return n;
                    }

                    public boolean isEmpty() {
                        return !iterator().hasNext();
                    }

                    public boolean remove(Object o) {
                        if (!(o instanceof Map.Entry)) {
                            return false;
                        }
                        Map.Entry entry = (Map.Entry)o;
                        Object key = entry.getKey();
                        if (!inRange(key)) {
                            return false;
                        }
                        Object value = entry.getValue();
                        if (value != null) { 
                            Object v = PersistentMapImpl.this.get(key);
                            if (value.equals(v)) {
                                PersistentMapImpl.this.remove(key);
                                return true;
                            }
                        } else {
                            if (PersistentMapImpl.this.containsKey(key)
                                && PersistentMapImpl.this.get(key) == null)
                            {
                                PersistentMapImpl.this.remove(key);
                                return true;
                            }
                        }
                        return false;
                    }

                    public boolean contains(Object k) {
                        Entry e = (Entry)k;
                        if (!inRange(e.getKey())) {
                            return false;
                        }                        
                        if (e.getValue() != null) { 
                            return e.getValue().equals(PersistentMapImpl.this.get(e.getKey()));
                        } else {
                            return PersistentMapImpl.this.containsKey(e.getKey()) 
                                && PersistentMapImpl.this.get(e.getKey()) == null;
                        }
                    }
                };
            }
            return entrySet;
        }   

        public SortedMap subMap(Object from, Object to) {
            if (!inRange2(from)) {
                throw new IllegalArgumentException("'from' out of range");
            }
            if (!inRange2(to)) {
                throw new IllegalArgumentException("'to' out of range");
            }
            return new SubMap(from, to);
        }

        public SortedMap headMap(Object to) {
            if (!inRange2(to)) {
                throw new IllegalArgumentException("'to' out of range");
            }
            return new SubMap(this.from, to);
        }

        public SortedMap tailMap(Object from) {
            if (!inRange2(from)) {
                throw new IllegalArgumentException("'from' out of range");
            }
            return new SubMap(from, this.to);
        }

        private boolean inRange(Object key) {
            Comparable k = (Comparable)key;
            return (from == null || k.compareTo(from) >= 0) &&
                (to == null || k.compareTo(to) < 0);
        }

        // This form allows the high endpoint (as well as all legit keys)
        private boolean inRange2(Object key) {
            Comparable k = (Comparable)key;
            return (from == null || k.compareTo(from) >= 0) &&
                (to == null || k.compareTo(to) <= 0);
        }
    }

    public Object firstKey() {
        if (index != null) { 
            if (type == ClassDescriptor.tpRaw) {               
                return ((Entry)((SortedCollection)index).iterator().next()).getKey();
            } else { 
                return ((Entry)((Index)index).entryIterator().next()).getKey();
            }
        } else { 
            Comparable[] keys = (Comparable[])this.keys;
            if (values.size() == 0) {
                throw new NoSuchElementException(); 
            }
            return ((Comparable[])keys)[0];
        }
    }

    public Object lastKey() {
        if (index != null) { 
            if (type == ClassDescriptor.tpRaw) {       
                IPersistent[] entries = ((SortedCollection)index).toPersistentArray();
                return ((Entry)entries[entries.length-1]).getKey();
            } else { 
                return ((Entry)((Index)index).entryIterator(null, null, Index.DESCENT_ORDER).next()).getKey();
            }
        } else { 
            int size = values.size();
            if (size == 0) {
                throw new NoSuchElementException(); 
            }
            return ((Comparable[])keys)[size-1];
        }
    }

    public Iterator select(Class cls, String predicate) { 
        Query query = new QueryImpl(getStorage());
        return query.select(cls, values().iterator(), predicate);
    }
} 
