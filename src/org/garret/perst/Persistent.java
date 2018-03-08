package org.garret.perst;
import org.garret.perst.impl.StorageImpl;

/**
 * Base class for all persistent capable objects
 */
public class Persistent extends PinnedPersistent
{ 
    public Persistent() {}

    public Persistent(Storage storage) { 
        super(storage);
    }

    protected void finalize() { 
        if ((state & DIRTY) != 0 && oid != 0) { 
            storage.storeFinalizedObject(this);
        }
        state = DELETED;
    }
}





