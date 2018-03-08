package org.garret.perst.continuous;

import org.garret.perst.*;


class RootObject extends PersistentResource 
{ 
    FieldIndex<TableDescriptor> tables;
    PerstDirectory.PerstCatalogue catalogue;
    long transId;
    IPersistent userData;

    synchronized long getLastTransactionId() 
    { 
        return transId;
    }

    synchronized long newTransactionId() 
    { 
        long id = ++transId;
        store();
        return id;
    }

    void setCatalogue(PerstDirectory.PerstCatalogue catalogue) 
    { 
        this.catalogue = catalogue;
        store();
    }

    RootObject(Storage storage) 
    { 
        super(storage);
        transId = 0;
        tables = storage.<TableDescriptor>createFieldIndex(TableDescriptor.class, "className", true);
    }

    RootObject() {}
}
    