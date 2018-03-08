package org.garret.perst.impl;
import  org.garret.perst.*;
import  java.util.NoSuchElementException;

public class JoinSetIterator extends Iterator
{ 
    private Iterator i1;
    private Iterator i2;
    private int currOid;
    private Storage storage;

    JoinSetIterator(Storage storage, Iterator left, Iterator right) { 
        this.storage = storage;
        i1 = left;
        i2 = right;
    }
    
    public boolean hasNext() { 
        if (currOid == 0) { 
            int oid1, oid2 = 0;
            while ((oid1 = i1.nextOid()) != 0) { 
                while (oid1 > oid2) { 
                    if ((oid2 = i2.nextOid()) == 0) {
                        return false;
                    }
                }
                if (oid1 == oid2) { 
                    currOid = oid1;
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
        return storage.getObjectByOID(currOid);
    }
    
    public int nextOid() { 
        return hasNext() ? currOid : 0;
    }
    
    public void remove() {
        throw new org.garret.perst.UnsupportedOperationException();
    }
}           
