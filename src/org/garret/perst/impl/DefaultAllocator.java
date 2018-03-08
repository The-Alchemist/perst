package org.garret.perst.impl;

import org.garret.perst.*;

public class DefaultAllocator extends Persistent implements CustomAllocator { 
    public DefaultAllocator(Storage storage) { 
        super(storage);
    }
    
    protected DefaultAllocator() {}

    public long allocate(long size) { 
        return ((StorageImpl)getStorage()).allocate(size, 0);
    }

    public long getSegmentBase() { 
        return 0;
    }

    public long getSegmentSize() {
        return 1L << StorageImpl.dbLargeDatabaseOffsetBits;
    }

    public long getUsedMemory() { 
        return getStorage().getUsedSize();
    }
    
    public long getAllocatedMemory() { 
        return getStorage().getDatabaseSize();
    }

    public long reallocate(long pos, long oldSize, long newSize) {
        StorageImpl db = (StorageImpl)getStorage();
        if (((newSize + StorageImpl.dbAllocationQuantum - 1) & ~(StorageImpl.dbAllocationQuantum-1))
            > ((oldSize + StorageImpl.dbAllocationQuantum - 1) & ~(StorageImpl.dbAllocationQuantum-1)))
        { 
            long newPos = db.allocate(newSize, 0);
            db.cloneBitmap(pos, oldSize);
            db.free(pos, oldSize);
            pos = newPos;
        }
        return pos;
    }

    public void free(long pos, long size) { 
        ((StorageImpl)getStorage()).cloneBitmap(pos, size);
    }
        
    public void commit() {}
}
        

