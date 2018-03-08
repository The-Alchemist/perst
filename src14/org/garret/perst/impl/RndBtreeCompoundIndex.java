package org.garret.perst.impl;
import  org.garret.perst.*;

import java.lang.reflect.*;
import java.util.*;
import java.io.Serializable;

class RndBtreeCompoundIndex extends RndBtree implements Index { 
    int[]    types;

    RndBtreeCompoundIndex() {}
    
    RndBtreeCompoundIndex(Class[] keyTypes, boolean unique) {
        this.unique = unique;
        type = ClassDescriptor.tpRaw;        
        types = new int[keyTypes.length];
        for (int i = 0; i < keyTypes.length; i++) {
            types[i] = getCompoundKeyComponentType(keyTypes[i]);
        }
    }

    static int getCompoundKeyComponentType(Class c) { 
        if (c.equals(Boolean.class)) { 
            return ClassDescriptor.tpBoolean;
        } else if (c.equals(Byte.class)) { 
            return ClassDescriptor.tpByte;
        } else if (c.equals(Character.class)) { 
            return ClassDescriptor.tpChar;
        } else if (c.equals(Short.class)) { 
            return ClassDescriptor.tpShort;
        } else if (c.equals(Integer.class)) { 
            return ClassDescriptor.tpInt;
        } else if (c.equals(Long.class)) { 
            return ClassDescriptor.tpLong;
        } else if (c.equals(Float.class)) { 
            return ClassDescriptor.tpFloat;
        } else if (c.equals(Double.class)) { 
            return ClassDescriptor.tpDouble;
        } else if (c.equals(String.class)) { 
            return ClassDescriptor.tpString;
        } else if (c.equals(Date.class)) { 
            return ClassDescriptor.tpDate;
        } else if (IPersistent.class.isAssignableFrom(c)) {
            return ClassDescriptor.tpObject;
        } else if (Comparable.class.isAssignableFrom(c)) { 
            return ClassDescriptor.tpRaw;
        } else { 
            throw new StorageError(StorageError.UNSUPPORTED_INDEX_TYPE, c);
        }
    }

    public Class[] getKeyTypes() {
        Class[] keyTypes = new Class[types.length];
        for (int i = 0; i < keyTypes.length; i++) { 
            keyTypes[i] = mapKeyType(types[i]);
        }
        return keyTypes;
    }

    static class CompoundKey implements Comparable, Serializable {
        Object[] keys;

        public int compareTo(Object o) { 
            CompoundKey c = (CompoundKey)o;
            int n = keys.length < c.keys.length ? keys.length : c.keys.length; 
            for (int i = 0; i < n; i++) { 
                int diff = ((Comparable)keys[i]).compareTo(c.keys[i]);
                if (diff != 0) { 
                    return diff;
                }
            }
            return 0; // allow to compare part of the compound key
        }

        CompoundKey(Object[] keys) { 
            this.keys = keys;
        }
    }
                
    private Key convertKey(Key key) { 
        if (key == null) { 
            return null;
        }
        if (key.type != ClassDescriptor.tpArrayOfObject) { 
            throw new StorageError(StorageError.INCOMPATIBLE_KEY_TYPE);
        }
        return new Key(new CompoundKey((Object[])key.oval), key.inclusion != 0);
    }
            
    public IPersistent[] get(Key from, Key till) {
        return super.get(convertKey(from), convertKey(till));
    }

    public IPersistent get(Key key) {
        return super.get(convertKey(key));
    }

    public IPersistent  remove(Key key) { 
        return super.remove(convertKey(key));
    }

    public void remove(Key key, IPersistent obj) { 
        super.remove(convertKey(key), obj);
    }

    public IPersistent set(Key key, IPersistent obj) { 
        return super.set(convertKey(key), obj);
    }

    public boolean put(Key key, IPersistent obj) {
        return super.put(convertKey(key), obj);
    }

    public Iterator iterator(Key from, Key till, int order) {
        return super.iterator(convertKey(from), convertKey(till), order);
    }

    public Iterator entryIterator(Key from, Key till, int order) {
        return super.entryIterator(convertKey(from), convertKey(till), order);
    }
    
    public int indexOf(Key key) { 
        return super.indexOf(convertKey(key));
    }
}

