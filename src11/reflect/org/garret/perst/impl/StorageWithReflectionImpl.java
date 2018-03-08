package org.garret.perst.impl;

import org.garret.perst.*;
import org.garret.perst.reflect.*;

public class StorageWithReflectionImpl extends StorageImpl 
{ 
    public synchronized FieldIndex createFieldIndex(Class type, String fieldName, boolean unique, boolean caseInsensitive) {
        if (!opened) { 
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }        
        FieldIndex index = caseInsensitive
            ? alternativeBtree
                ? (FieldIndex)new AltBtreeCaseInsensitiveFieldIndex(type, fieldName, unique)
                : (FieldIndex)new BtreeCaseInsensitiveFieldIndex(type, fieldName, unique)
            : alternativeBtree
                ? (FieldIndex)new AltBtreeFieldIndex(type, fieldName, unique)
                : (FieldIndex)new BtreeFieldIndex(type, fieldName, unique);
        index.assignOid(this, 0, false);
        return index;
    }
    
    public synchronized FieldIndex createFieldIndex(Class type, String[] fieldNames, boolean unique, boolean caseInsensitive) {
        if (!opened) { 
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }        
        if (alternativeBtree) { 
            throw new StorageError(StorageError.UNSUPPORTED_INDEX_TYPE);
        }                    
        FieldIndex index = caseInsensitive
            ? (FieldIndex)new BtreeCaseInsensitiveMultiFieldIndex(type, fieldNames, unique)
            : (FieldIndex)new BtreeMultiFieldIndex(type, fieldNames, unique);
        index.assignOid(this, 0, false);
        return index;
    }

    public synchronized FieldIndex createRandomAccessFieldIndex(Class type, String fieldName, boolean unique, boolean caseInsensitive) {
        if (!opened) { 
            throw new StorageError(StorageError.STORAGE_NOT_OPENED);
        }        
        FieldIndex index = caseInsensitive
            ? (FieldIndex)new RndBtreeCaseInsensitiveFieldIndex(type, fieldName, unique)
            : (FieldIndex)new RndBtreeFieldIndex(type, fieldName, unique);
        index.assignOid(this, 0, false);
        return index;
    }

    public MultidimensionalIndex createMultidimensionalIndex(Class type, String[] fieldNames, boolean treateZeroAsUndefinedValue)
    {
        return createMultidimensionalIndex(new ReflectionMultidimensionalComparator(this, type, fieldNames, treateZeroAsUndefinedValue));
    }      

    public Query createQuery() { 
        return new QueryImpl(this);
    }

    Object newInstance(Class cls) { 
        Type type = ReflectionProvider.getInstance().getType(cls);
        try { 
            return (type != null) ? type.newInstance() : super.newInstance(cls);
        } catch (Exception x) { 
            throw new StorageError(StorageError.CONSTRUCTOR_FAILURE, cls, x);
        }
    }
}