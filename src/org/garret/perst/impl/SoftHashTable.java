package org.garret.perst.impl;
import  java.lang.ref.*;

public class SoftHashTable extends WeakHashTable { 
    public SoftHashTable(StorageImpl db, int initialCapacity) {
        super(db, initialCapacity);
    }
    
    protected Reference createReference(Object obj) { 
        return new SoftReference(obj);
    }
}    
