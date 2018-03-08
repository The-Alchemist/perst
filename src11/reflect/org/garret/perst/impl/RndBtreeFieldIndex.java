package org.garret.perst.impl;
import  org.garret.perst.*;

import org.garret.perst.reflect.*;
import java.util.Date;

class RndBtreeFieldIndex extends RndBtree implements FieldIndex { 
    String className;
    String fieldName;
    long   autoincCount;
    transient Type  cls;
    transient Field fld;

    public RndBtreeFieldIndex() {}
    
    public void writeObject(IOutputStream out) {
        super.writeObject(out);
        out.writeString(className);
        out.writeString(fieldName);
        out.writeLong(autoincCount);
    }

    public void readObject(IInputStream in) {
        super.readObject(in);
        className = in.readString();
        fieldName = in.readString();
        autoincCount = in.readLong();
        cls = ReflectionProvider.getInstance().getType(className);
        locateField();
    }

    private final void locateField() 
    {
        fld = cls.getField(fieldName);
        if (fld == null) { 
           throw new StorageError(StorageError.INDEXED_FIELD_NOT_FOUND, className + "." + fieldName);
        }
    }

    public Class getIndexedClass() { 
        return cls.getDescribedClass();
    }

    public String[] getKeyFields() { 
        return new String[]{fieldName};
    }

    RndBtreeFieldIndex(Class cls, String fieldName, boolean unique) {
        this.cls = ReflectionProvider.getInstance().getType(cls);
        this.unique = unique;
        this.fieldName = fieldName;
        this.className = cls.getName();
        locateField();
        type = checkType(Types.getTypeCode(fld.getType()));
    }

    private Key extractKey(IPersistent obj) { 
        try { 
            Field f = fld;
            Key key = null;
            switch (type) {
              case Types.Boolean:
                key = new Key(f.getBoolean(obj));
                break;
              case Types.Byte:
                key = new Key(f.getByte(obj));
                break;
              case Types.Short:
                key = new Key(f.getShort(obj));
                break;
              case Types.Char:
                key = new Key(f.getChar(obj));
                break;
              case Types.Int:
                key = new Key(f.getInt(obj));
                break;            
              case Types.Object:
                {
                    IPersistent ptr = (IPersistent)f.get(obj);
                    if (ptr != null && !ptr.isPersistent())
                    {
                        getStorage().makePersistent(ptr);
                    }
                    key = new Key(ptr);
                    break;
                }
              case Types.Long:
                key = new Key(f.getLong(obj));
                break;            
              case Types.Date:
                key = new Key((Date)f.get(obj));
                break;
              case Types.Float:
                key = new Key(f.getFloat(obj));
                break;
              case Types.Double:
                key = new Key(f.getDouble(obj));
                break;
              case Types.String:
                {
                    Object val = f.get(obj);
                    if (val != null) { 
                        key = new Key((String)val);
                    }
                }
                break;
              case Types.ArrayOfByte:
                key = new Key((byte[])f.get(obj));
                break;
              default:
                Assert.failed("Invalid type");
            }
            return key;
        } catch (Exception x) { 
            throw new StorageError(StorageError.ACCESS_VIOLATION, x);
        }
    }
            

    public boolean put(IPersistent obj) {
        Key key = extractKey(obj);
        return key != null && super.insert(key, obj, false) == null;
    }

    public IPersistent set(IPersistent obj) {
        Key key = extractKey(obj);
        if (key == null) {
            throw new StorageError(StorageError.KEY_IS_NULL);
        }
        return super.set(key, obj);
    }

    public void  remove(IPersistent obj) {
        Key key = extractKey(obj);
        if (key != null) { 
            super.remove(key, obj);
        }
    }

    public boolean containsObject(IPersistent obj) {
        Key key = extractKey(obj);
        if (key == null) { 
            return false;
        }
        if (unique) { 
            return super.get(key) != null;
        } else { 
            IPersistent[] mbrs = get(key, key);
            for (int i = 0; i < mbrs.length; i++) { 
                if (mbrs[i] == obj) { 
                    return true;
                }
            }
            return false;
        }
    }

    public boolean contains(IPersistent obj) {
        Key key = extractKey(obj);
        if (key == null) { 
            return false;
        }
        if (unique) { 
            return super.get(key) != null;
        } else { 
            IPersistent[] mbrs = get(key, key);
            for (int i = 0; i < mbrs.length; i++) { 
                if (mbrs[i].equals(obj)) { 
                    return true;
                }
            }
            return false;
        }
    }

    public synchronized void append(IPersistent obj) {
        Key key;
        try { 
            switch (type) {
              case Types.Int:
                key = new Key((int)autoincCount);
                fld.setInt(obj, (int)autoincCount);
                break;            
              case Types.Long:
                key = new Key(autoincCount);
                fld.setLong(obj, autoincCount);
                break;            
              default:
                throw new StorageError(StorageError.UNSUPPORTED_INDEX_TYPE, fld.getType());
            }
        } catch (Exception x) { 
            throw new StorageError(StorageError.ACCESS_VIOLATION, x);
        }
        autoincCount += 1;
        obj.modify();
        super.insert(key, obj, false);
    }

    public IPersistent[] get(Key from, Key till) {
        ArrayList list = new ArrayList();
        if (root != null) { 
            root.find(checkKey(from), checkKey(till), height, list);
        }
        return (IPersistent[])list.toArray(new IPersistent[list.size()]);
    }

    public IPersistent[] toPersistentArray() {
        IPersistent[] arr = new IPersistent[nElems];
        if (root != null) { 
            root.traverseForward(height, arr, 0);
        }
        return arr;
    }


    public boolean isCaseInsensitive() { 
        return false;
    }
}

class RndBtreeCaseInsensitiveFieldIndex extends RndBtreeFieldIndex {    
    RndBtreeCaseInsensitiveFieldIndex() {}

    RndBtreeCaseInsensitiveFieldIndex(Class cls, String fieldName, boolean unique) {
        super(cls, fieldName, unique);
    }

    Key checkKey(Key key) { 
        if (key != null && key.oval instanceof String) { 
            key = new Key(((String)key.oval).toLowerCase(), key.inclusion != 0);
        }
        return super.checkKey(key);
    }  

    public boolean isCaseInsensitive() { 
        return true;
    }
}