package org.garret.perst.impl;
import  org.garret.perst.*;

import java.lang.reflect.*;
import java.util.*;

class ThickFieldIndex<T> extends ThickIndex<T> implements FieldIndex<T> 
{
    String fieldName;
    int type;
    Class cls;
    transient Field fld;
    
    static Field locateField(Class cls, String fieldName) {
        Field fld = ClassDescriptor.locateField(cls, fieldName);
        if (fld == null) { 
            throw new StorageError(StorageError.INDEXED_FIELD_NOT_FOUND, cls.getName() + "." + fieldName);
        }
        return fld;
    }

    private final void locateField() {
        fld = locateField(cls, fieldName);
    }
    
    public Class getIndexedClass() { 
        return cls;
    }

    public Field[] getKeyFields() { 
        return new Field[]{fld};
    }

    public void onLoad()
    {
        locateField();
    }

    ThickFieldIndex() {}

    ThickFieldIndex(StorageImpl db, Class cls, String fieldName) {
        this(db, cls, fieldName, locateField(cls, fieldName));
    }

    ThickFieldIndex(StorageImpl db, Class cls, String fieldName, Field fld) {
        super(db, fld.getType());
        type = Btree.checkType(fld.getType());
        this.cls = cls;
        this.fld = fld;
        this.fieldName = fieldName;
    }

    protected String transformStringKey(String key) { 
        return key;
    }

    protected Key transformKey(Key key) { 
        return key;
    }  

    private Key extractKey(Object obj) { 
        try { 
            Field f = fld;
            Key key = null;
            switch (type) {
              case ClassDescriptor.tpBoolean:
                key = new Key(f.getBoolean(obj));
                break;
              case ClassDescriptor.tpByte:
                key = new Key(f.getByte(obj));
                break;
              case ClassDescriptor.tpShort:
                key = new Key(f.getShort(obj));
                break;
              case ClassDescriptor.tpChar:
                key = new Key(f.getChar(obj));
                break;
              case ClassDescriptor.tpInt:
                key = new Key(f.getInt(obj));
                break;            
              case ClassDescriptor.tpObject:
                {
                    Object val = f.get(obj);
                    key = new Key(val, getStorage().makePersistent(val), true);
                    break;
                }
              case ClassDescriptor.tpLong:
                key = new Key(f.getLong(obj));
                break;            
              case ClassDescriptor.tpDate:
                key = new Key((Date)f.get(obj));
                break;
              case ClassDescriptor.tpFloat:
                key = new Key(f.getFloat(obj));
                break;
              case ClassDescriptor.tpDouble:
                key = new Key(f.getDouble(obj));
                break;
              case ClassDescriptor.tpEnum:
                key = new Key((Enum)f.get(obj));
                break;
              case ClassDescriptor.tpString:
                {
                    Object val = f.get(obj);
                    if (val != null) { 
                        key = new Key(transformStringKey((String)val));
                    }
                }
                break;
              default:
                Assert.failed("Invalid type");
            }
            return key;
        } catch (Exception x) { 
            throw new StorageError(StorageError.ACCESS_VIOLATION, x);
        }
    }
            
    public boolean add(T obj) {
        return put(obj);
    }

    public boolean put(T obj) { 
        Key key = extractKey(obj);
        return key != null && super.put(key, obj);
    }

    public T set(T obj) {
        Key key = extractKey(obj);
        if (key == null) {
            throw new StorageError(StorageError.KEY_IS_NULL);
        }
        return super.set(key, obj);
    }

    public boolean addAll(Collection<? extends T> c) {
        FieldValue[] arr = new FieldValue[c.size()];
	Iterator<? extends T> e = c.iterator();
        try { 
            for (int i = 0; e.hasNext(); i++) {
                T obj = e.next();
                arr[i] = new FieldValue(obj, fld.get(obj));
            }
        } catch (Exception x) { 
            throw new StorageError(StorageError.ACCESS_VIOLATION, x);
        }
        Arrays.sort(arr);
	for (int i = 0; i < arr.length; i++) {
            add((T)arr[i].obj);
        }
	return arr.length > 0;
    }

    public boolean remove(Object obj) {
        Key key = extractKey(obj);
        return key != null && super.removeIfExists(key, (T)obj);
    }

    public boolean containsObject(T obj) {
        Key key = extractKey(obj);
        if (key == null) { 
            return false;
        }
        Object[] mbrs = get(key, key);
        for (int i = 0; i < mbrs.length; i++) { 
            if (mbrs[i] == obj) { 
                return true;
            }
        }
        return false;
    }

    public boolean contains(Object obj) {
        Key key = extractKey(obj);
        if (key == null) { 
            return false;
        }
        Object[] mbrs = get(key, key);
        for (int i = 0; i < mbrs.length; i++) { 
            if (mbrs[i].equals(obj)) { 
                return true;
            }
        }
        return false;
    }

    public void append(T obj) {
        throw new UnsupportedOperationException();
    }   

    public T[] getPrefix(String prefix) { 
        return (T[])super.getPrefix(transformStringKey(prefix));
    }

    protected Object[] extend(Object[] s) { 
        ArrayList list = new ArrayList();
        for (int i = 0; i < s.length; i++) { 
            list.addAll((Collection)s[i]);
        }
        return list.toArray((T[])Array.newInstance(cls, list.size()));        
    }
         
    public T[] prefixSearch(String key) { 
        return (T[])super.prefixSearch(transformStringKey(key));
    }

    public T[] get(Key from, Key till) {
        return (T[])super.get(transformKey(from), transformKey(till));
    }

    public T[] get(Object from, Object till) {
        return (T[])super.get(from, till);
    }

    public T[] toArray() {
        return (T[])super.toArray();
    }

    public IterableIterator<T> queryByExample(T obj) {
        Key key = extractKey(obj);
        return iterator(key, key, ASCENT_ORDER);
    }
            
    public IterableIterator<T> select(String predicate) { 
        Query<T> query = new QueryImpl<T>(getStorage());
        return query.select(cls, iterator(), predicate);
    }

    public boolean isCaseInsensitive() { 
        return false;
    }
}

class ThickCaseInsensitiveFieldIndex<T>  extends ThickFieldIndex<T> {    
    ThickCaseInsensitiveFieldIndex() {}

    ThickCaseInsensitiveFieldIndex(StorageImpl db, Class cls, String fieldName) {
        super(db, cls, fieldName);
    }

    protected String transformStringKey(String key) { 
        return key.toLowerCase();
    }

    protected Key transformKey(Key key) { 
        if (key != null && key.oval instanceof String) { 
            key = new Key(((String)key.oval).toLowerCase(), key.inclusion != 0);
        }
        return key;
    }  

    public T get(Key key) {
        return super.get(transformKey(key));
    }

    public ArrayList<T> getList(Key from, Key till) { 
        return super.getList(transformKey(from), transformKey(till));
    }

    public ArrayList<T> getPrefixList(String prefix) { 
        return super.getPrefixList(transformStringKey(prefix));
    }

    public ArrayList<T> prefixSearchList(String word) { 
        return super.prefixSearchList(transformStringKey(word));
    }

    public IterableIterator<T> iterator(Key from, Key till, int order) { 
        return super.iterator(transformKey(from), transformKey(till), order);
    }

    public  IterableIterator<Map.Entry<Object,T>> entryIterator(Key from, Key till, int order) { 
        return super.entryIterator(transformKey(from), transformKey(till), order);
    }

    public IterableIterator<T> prefixIterator(String prefix, int order) { 
        return super.prefixIterator(transformStringKey(prefix), order);
    }

    public int indexOf(Key key) { 
        return super.indexOf(transformKey(key));
    }

    public boolean isCaseInsensitive() { 
        return true;
    }
}
