package org.garret.perst.impl;
import  org.garret.perst.*;
import  java.util.*;

class PersistentMapImpl extends PersistentResource implements IPersistentMap
{
    Index    index;
    Object[] keys;
    Link     values;
    int      type;
    int      nElems;

    PersistentMapImpl(Storage storage, int keyType, int smallHashSize) { 
        super(storage);
        type = keyType;
        keys = new Object[smallHashSize];
        values = storage.createLink(smallHashSize);
        values.setSize(smallHashSize);
    }
    
    public PersistentMapImpl() {}

    public void writeObject(IOutputStream out) {
        out.writeInt(type);
        out.writeObject(index);
        if (index == null) {
            out.writeInt(nElems);
            out.writeLink(values);
            for (int i = 0; i < keys.length; i++) { 
                out.write(keys[i]);
            }
        }
    }

    public void readObject(IInputStream in) {
        type = in.readInt();
        index = (Index)in.readObject();
        if (index == null) { 
            nElems = in.readInt();
            values = in.readLink();
            keys = new Object[values.size()];
            for (int i = 0; i < keys.length; i++) { 
                keys[i] = in.read();
            }        
        }
    }
    
    public int size() {
        return index != null ? index.size() : nElems;
    }

    public boolean isEmpty() {
	return size() == 0;
    }

    public boolean contains(Object value) {
        Iterator i = elements();
        while (i.hasNext()) { 
            if (i.next().equals(value)) { 
                return true;
            }
        }
        return false;
    }

    public boolean containsKey(Object key) {
        if (index != null) { 
            return index.get(generateKey(key)) != null;
        } else {
            int len = keys.length;
            int h = (int)((key.hashCode() & 0xFFFFFFFFL) % len);
            while (keys[h] != null) { 
                if (keys[h].equals(key)) {
                    return true;
                }
                h = (h + 1) % len;
            }
            return false;
        }
    }

    public Object get(Object key) {
        if (index != null) { 
            return index.get(generateKey(key));
        } else { 
            int len = keys.length;
            int h = (int)((key.hashCode() & 0xFFFFFFFFL) % len);
            while (keys[h] != null) { 
                if (keys[h].equals(key)) {
                    return values.get(h);
                }
                h = (h + 1) % len;
            }
            return null;
        }
    }

    class SmallHashIterator extends Iterator 
    {
        Map.Entry currEntry;
        int i = -1;

        public boolean hasNext() { 
            if (currEntry == null) { 
                int n = keys.length;
                while (++i < n) { 
                    if (keys[i] != null) { 
                        currEntry = new Map.Entry() { 
                            public Object getKey() { 
                                return keys[i];
                            }
                            public Object getValue() { 
                                return values.get(i);
                            }
                        };
                        return true;
                    }
                }
                return false;
            }
            return true;
        }
        
        public Object next() { 
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Object curr = currEntry;
            currEntry = null;
            return curr;
        }
    
        public int nextOid() { 
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return values.getRaw(i).getOid();
        }

        public void remove() { 
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            keys[i] = null;
            values.set(i, null);
            nElems -= 1;
            modify();
            currEntry = null;
        }
    }
            
    public Iterator iterator() {
        return (index != null) ? index.entryIterator() : new SmallHashIterator();
    }            

    static class ValueIterator extends Iterator 
    {
        Iterator entryIterator;

        ValueIterator(Iterator entryIterator) {
            this.entryIterator = entryIterator;
        }
        
        public boolean hasNext() {
            return entryIterator.hasNext();
        }

        public Object next() {
            return ((Map.Entry)entryIterator.next()).getValue();
        }
        
        public void remove() {
            entryIterator.remove();
        }

        public int nextOid() { 
            return ((IPersistent)next()).getOid();
        }
    }

    static class KeyIterator extends ValueIterator {
        KeyIterator(Iterator entryIterator) {
            super(entryIterator);
        }
        
        public Object next() {
            return ((Map.Entry)entryIterator.next()).getKey();
        }
    }


    public Iterator elements() { 
        return new ValueIterator(iterator());
    }
        
    public Iterator keys() { 
        return new KeyIterator(iterator());
    }
        

    public Object put(Object key, Object value) {
        if (index != null) { 
            return index.set(generateKey(key), (IPersistent)value);
        } else { 
            int len = keys.length;
            int h = (int)((key.hashCode() & 0xFFFFFFFFL) % len);
            while (keys[h] != null) { 
                if (keys[h].equals(key)) {
                    Object old = values.get(h);
                    values.set(h, (IPersistent)value);
                    modify();
                    return old;
                }
                h = (h + 1) % len;
            }
            if (++nElems > len/2) { 
                index = getStorage().createIndex(type, true);
                for (int i = 0; i < len; i++) {
                    if (keys[i] != null) {
                        index.put(generateKey(keys[i]), values.getRaw(i));
                    }
                }
                values = null;
                keys = null;
                index.set(generateKey(key), (IPersistent)value);
            } else { 
                keys[h] = key;
                values.set(h, (IPersistent)value);
            }
            modify();            
            return null;
        }
    }
        
    public Object remove(Object key) {
        if (index != null) { 
            try { 
                return index.remove(generateKey(key));
            } catch (StorageError x) { 
                if (x.getErrorCode() == StorageError.KEY_NOT_FOUND) {
                    return null;
                } else { 
                    throw x;
                }
            }
        } else { 
            int len = keys.length;
            int h = (int)((key.hashCode() & 0xFFFFFFFFL) % len);
            while (keys[h] != null) { 
                if (keys[h].equals(key)) {
                    Object old = values.get(h);
                    values.set(h, null);
                    keys[h] = null;
                    nElems -= 1;
                    modify();
                    return old;
                }
                h = (h + 1) % len;
            }
            return null;
        }
    }

    public void clear() {
        if (index != null) { 
            index.clear();
        } else {
            values.clear();
            values.setSize(keys.length);
            keys = new Object[keys.length];
            nElems = 0;
            modify();
        }
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
            Iterator i = iterator();
            while (i.hasNext()) {
                Map.Entry e = (Map.Entry)i.next();
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
	Iterator i = elements();
	while (i.hasNext()) {
	    h += i.next().hashCode();
        }
	return h;
    }

    public String toString() {
	StringBuffer buf = new StringBuffer();
	buf.append("{");

	Iterator i = iterator();
        boolean hasNext = i.hasNext();
        while (hasNext) {
	    Map.Entry e = (Map.Entry)i.next();
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
        switch (type) { 
        case Types.Boolean:
            return new Key(((Boolean)key).booleanValue());
        case Types.Byte:
            return new Key(((Byte)key).byteValue());
        case Types.Char:
            return new Key(((Character)key).charValue());
        case Types.Short:
            return new Key(((Short)key).shortValue());
        case Types.Int:
            return new Key(((Integer)key).intValue());
        case Types.Long:
            return new Key(((Long)key).longValue());
        case Types.Float:
            return new Key(((Float)key).floatValue());
        case Types.Double:
            return new Key(((Double)key).doubleValue());
        case Types.String:
            return new Key((String)key);
        case Types.Date:
            return new Key((Date)key);
        case Types.Object:
            return new Key((IPersistent)key);
        default:
            throw new StorageError(StorageError.UNSUPPORTED_INDEX_TYPE);
        }
    }
} 
