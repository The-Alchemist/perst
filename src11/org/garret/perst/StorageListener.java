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
     * This method is called when garbage collection is  started (ether explicitly
     * by invocation of Storage.gc() method, either implicitly  after allocation
     * of some amount of memory)).
     */
    public void gcStarted() {}

    /**
     * This method is called  when unreferenced object is deallocated from 
     * database. It is possible to get instance of the object using
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
     * This method is called by XML exporter if database correuption or some other reasons
     * makes export of the object not possible
     * @param oid object identified 
     * @param x catched exception
     */
    public void objectNotExported(int oid, StorageError x) {}

    /**
     * This method is called when runtime error happen during execution of JSQL query
     */
    public void JSQLRuntimeError(RuntimeException x) { 
    }

    /**
     * Sequential search is performed for query execution
     * @param table table in which sequential search is perfomed.
     * @param query executed query
     */
    public void sequentialSearchPerformed(Class table, String query) {
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
     * Sort of the selected result set is performed for query execution
     * @param table table in which sort is perfomed.
     * @param query executed query
     */
    public void sortResultSetPerformed(Class table, String query) {
    }        

    /**
     * Index is automaticaslly created by Database class when query is executed and autoIndices is anabled
     * @param table table for which index is created
     * @param field index key
     */
    public void indexCreated(Class table, String field) {
    }
}
     
