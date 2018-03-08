package org.garret.perst;

/**
 * Listener of database events. Programmer should derive his own subclass and register
 * it using Storage.setListener method.
 */
public abstract class StorageListener {
    /**
     * This metod is called during database open when database was not
     * close normally and has to be recovered
     */
    public void databaseCorrupted() {}

    /**
     * This method is called after completion of recovery
     */
    public void recoveryCompleted() {}

    /**
     * Method invoked by Perst affter object is loaded from the database 
     * @param obj loaded object
     */        
    public void onObjectLoad(Object obj) {}
            
    /**
     * Method invoked by Perst before object is written to the database 
     * @param obj stored object
     */        
    public void onObjectStore(Object obj) {}

    /**
     * Method invoked by Perst before object is deallocated
     * @param obj deallocated object
     */        
    public void onObjectDelete(Object obj) {}

    /**
     * Method invoked by Perst after object is assigned OID (becomes persisistent)
     * @param obj object which is made persistent
     */        
    public void onObjectAssignOid(Object obj) {}

    /**
     * Method invoked by Perst when slave node receive updates from master
     */
    public void onMasterDatabaseUpdate() {}

    /**
     * Method invoked by Perst when transaction is committed
     */
    public void onTransactionCommit() {}

    /**
     * Method invoked by Perst when transaction is aborted
     */
    public void onTransactionRollback() {}

    /**
     * This method is called when garbage collection is  started (ether explicitly
     * by invocation of Storage.gc() method, either implicitly  after allocation
     * of some amount of memory)).
     */
    public void gcStarted() {}

    /**
     * This method is called  when unreferenced object is deallocated from 
     * database during garbage collection. It is possible to get instance of the object using
     * <code>Storage.getObjectByOid()</code> method.
     * @param cls class of deallocated object
     * @param oid object identifier of deallocated object
     */
    public void deallocateObject(Class cls, int oid) {}

    /**
     * This method is called when garbage collection is completed
     * @param nDeallocatedObjects number of deallocated objects
     */
    public void gcCompleted(int nDeallocatedObjects) {}

    /**
     * This method is called by XMLExporter when exception happens during object export.
     * Exception can be caused either by IO problems (failed to write data to the destination stream) 
     * either by corruption of object.
     * @param oid object identifier of exported object
     * @param x catched exception
     * @return <b>true</b> if error should be ignored and export continued, <b>false</b> to rethrow catched exception
     */
    public boolean onXmlExportError(int oid, Exception x) 
    {
        return true;
    }

    /**
     * Handle replication error 
     * @param host address of host replication to which is failed (null if error jappens at slave node)
     * @return <code>true</code> if host should be reconnected and attempt to send data to it should be 
     * repeated, <code>false</code> if no more attmpts to communicate with this host should be performed 
     */
    public boolean replicationError(String host) {
        return false;
    }        

    /**
     * This method is called when runtime error happen during execution of JSQL query
     */
    public void JSQLRuntimeError(JSQLRuntimeException x) { 
    }

    /**
     * This method is called when query is executed
     * @param table table in which search is perfomed.
     * @param query executed query
     * @param elapsedTime time in milliseconds of query execution (please notice that many queries are executed
     * incrementally, so the time passed to this method is not always complete time of query execution     
     * @param sequentialSearch true if sequential scan in used for query execution, false if some index is used
     */
    public void queryExecution(Class table, String query, long elapsedTime, boolean sequentialSearch)  {
    }        

    /**
     * Sequential search is performed for query execution
     * @param table table in which sequential search is perfomed.
     * @param query executed query
     */
    public void sequentialSearchPerformed(Class table, String query) {
    }        

    /** 
     * Sort of the selected result set is performed for query execution
     * @param table table in which sort is perfomed.
     * @param query executed query
     */
    public void sortResultSetPerformed(Class table, String query) {
    }        

    /**
     * This method is called by XML exporter if database correuption or some other reasons
     * makes export of the object not possible
     * @param oid object identified 
     * @param x catched exception
     * @return <b>true</b> to ignore error and continue export, <b>false</b> to cancel export and rethrow exception
     */
    public boolean objectNotExported(int oid, StorageError x) {
        return false;
    }

    /**
     * Index is automaticaslly created by Database class when query is executed and autoIndices is anabled
     * @param table table for which index is created
     * @param field index key
     */
    public void indexCreated(Class table, String field) {}
}
     
