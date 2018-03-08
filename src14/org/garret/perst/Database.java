package org.garret.perst;

import java.util.*;
import org.garret.perst.impl.ClassDescriptor;
/**
 * This class emulates relational database on top of Perst storage
 * It maintain class extends, associated indices, prepare queries.
 */
public class Database { 
    /** 
     * Constructor of database. This method initialize database if it not initialized yet.
     * @param storage opened storage. Storage should be either empty (non-initialized, either
     * previously initialized by the this method. It is not possible to open storage with 
     * root object other than table index created by this constructor.
     * @param multithreaded <code>true</code> if database should support concurrent access
     * to the data from multiple threads.
     */
    public Database(Storage storage, boolean multithreaded) { 
        this.storage = storage;
        this.multithreaded = multithreaded;
        if (multithreaded) { 
            storage.setProperty("perst.alternative.btree", Boolean.TRUE);
        }
        metadata = (Index)storage.getRoot();
        if (metadata == null) { 
            beginTransaction();
            metadata = storage.createIndex(String.class, true);
            storage.setRoot(metadata);
            commitTransaction();
        }
        Iterator iterator = metadata.entryIterator();
        tables = new HashMap();
        while (iterator.hasNext()) { 
            Map.Entry map = (Map.Entry)iterator.next();
            Class type = ClassDescriptor.loadClass(storage, (String)map.getKey());
            Table table = (Table)map.getValue();
            table.type = type;
            tables.put(type, table);
        }
    }
        
    /** 
     * Constructor of single threaded database. This method initialize database if it not initialized yet.
     * @param storage opened storage. Storage should be either empty (non-initialized, either
     * previously initialized by the this method. It is not possible to open storage with 
     * root object other than table index created by this constructor.
     */
    public Database(Storage storage) { 
        this(storage, false);
    }
        
    /**
     * Begin transaction
     */
    public void beginTransaction() { 
        if (multithreaded) { 
            storage.beginThreadTransaction(Storage.SERIALIZABLE_TRANSACTION);
        }
    }

    /**
     * Commit transaction
     */
    public void commitTransaction() { 
        if (multithreaded) { 
            storage.endThreadTransaction();
        } else { 
            storage.commit();
        }
    }
    
    /**
     * Rollback transaction
     */
    public void rollbackTransaction() { 
        if (multithreaded) { 
            storage.rollbackThreadTransaction();
        } else { 
            storage.rollback();
        }
    }
    

    /**
     * Create table for the specified class.
     * This function does nothing if table for such class already exists
     * @param table class corresponding to the table
     * @return <code>true</code> if table is created, <code>false</code> if table 
     * alreay exists
     */
    public boolean createTable(Class table) { 
        if (multithreaded) { 
            metadata.exclusiveLock();
        }
        if (tables.get(table) == null) { 
            Table t = new Table();
            t.extent = storage.createSet();
            t.indices = storage.createLink();
            t.indicesMap = new HashMap();
            t.type = table;
            tables.put(table, t);
            metadata.put(table.getName(), t);
            return true;
        }
        return false;
    }
               
    /**
     * Drop table associated with this class. Do nothing if there is no such table in the database.
     * @param table class corresponding to the table
     * @return <code>true</code> if table is deleted, <code>false</code> if table 
     * is not found
     */
    public boolean dropTable(Class table) { 
        if (multithreaded) { 
            metadata.exclusiveLock();
        }
        if (tables.remove(table) != null) { 
            metadata.remove(table.getName());
            return true;
        }
        return false;
    }
        
    /**
     * Add new record to the table. Record is inserted in table corresponding to the class of the object.
     * Record will be automatically added to all indices existed for this table.
     * If there is no table associated with class of this object, then 
     * database will search for table associated with superclass and so on...
     * @param record object to be inserted in the table
     * @return <code>true</code> if record was successfully added to the table, <code>false</code>
     * if there is already such record (object with the same ID) in the table or there is some record with the same value of 
     * unique key field
     * @exception StorageError (CLASS_NOT_FOUND) exception is thrown if there is no table corresponding to 
     * record class
     */
    public boolean addRecord(IPersistent record) { 
        return addRecord(record.getClass(), record);        
    }

    private Table locateTable(Class cls, boolean exclusive) { 
        return locateTable(cls, exclusive, true);
    }

    private Table locateTable(Class cls, boolean exclusive, boolean shouldExist) { 
        Table table = null;
        if (multithreaded) { 
            metadata.sharedLock();
        }
        for (Class c = cls; c != null && (table = (Table)tables.get(c)) == null; c = c.getSuperclass());
        if (table == null) { 
            if (shouldExist) { 
                throw new StorageError(StorageError.CLASS_NOT_FOUND, cls.getName());
            }
            return null;
        }
        if (multithreaded) { 
            if (exclusive) { 
                table.extent.exclusiveLock();
            } else { 
                table.extent.sharedLock();
            }
        }
        return table;
    }

    /**
     * Add new record to the specified table. Record is inserted in table corresponding to the specified class.
     * Record will be automatically added to all indices existed for this table.
     * @param table class corresponding to the table
     * @param record object to be inserted in the table
     * @return <code>true</code> if record was successfully added to the table, <code>false</code>
     * if there is already such record (object with the same ID) in the table or there is some record with the same value of 
     * unique key field
     * @exception StorageError (CLASS_NOT_FOUND) exception is thrown if there is no table corresponding to 
     * record class
     */
    public boolean addRecord(Class table, IPersistent record) { 
        boolean added = false;
        boolean found = false;
        if (multithreaded) { 
            metadata.sharedLock();
        }
        ArrayList wasInsertedIn = new ArrayList();
        for (Class c = table; c != null; c = c.getSuperclass()) { 
            Table t = (Table)tables.get(c);
            if (t != null) { 
                found = true;
                if (multithreaded) { 
                    t.extent.exclusiveLock();
                }
                if (t.extent.add(record)) { 
                    wasInsertedIn.add(t.extent);
                    Iterator iterator = t.indicesMap.values().iterator();
                    while (iterator.hasNext()) { 
                        FieldIndex index = (FieldIndex)iterator.next();
                        if (index.put(record)) { 
                            wasInsertedIn.add(index);
                        } else { 
                            iterator = wasInsertedIn.iterator();
                            while (iterator.hasNext()) { 
                                Object idx = iterator.next();
                                if (idx instanceof IPersistentSet) {
                                    ((IPersistentSet)idx).remove(record);
                                } else { 
                                    ((FieldIndex)idx).remove(record);
                                }
                            }
                            return false;
                        }
                    }
                    added = true;
                }
            }
        }
        if (!found) { 
            throw new StorageError(StorageError.CLASS_NOT_FOUND, table.getName());
        }            
        return added;
    }
        
    /** 
     * Delete record from the table. Record is removed from the table corresponding to the class 
     * of the object. Record will be automatically added to all indices existed for this table.
     * If there is no table associated with class of this object, then 
     * database will search for table associated with superclass and so on...
     * Object represented the record will be also deleted from the storage.
     * @param record object to be deleted from the table
     * @return <code>true</code> if record was successfully deleted from the table, <code>false</code>
     * if there is not such record (object with the same ID) in the table
     * @exception StorageError (CLASS_NOT_FOUND) exception is thrown if there is no table corresponding to 
     * record class
     */
    public boolean deleteRecord(IPersistent record) { 
        return deleteRecord(record.getClass(), record);
    }

    /** 
     * Delete record from the specified table. Record is removed from the table corresponding to the 
     * specified class. Record will be automatically added to all indices existed for this table.
     * Object represented the record will be also deleted from the storage.
     * @param table class corresponding to the table
     * @param record object to be deleted from the table
     * @return <code>true</code> if record was successfully deleted from the table, <code>false</code>
     * if there is not such record (object with the same ID) in the table
     * @exception StorageError (CLASS_NOT_FOUND) exception is thrown if there is no table corresponding to 
     * specified class
     */
    public boolean deleteRecord(Class table, IPersistent record) { 
        boolean removed = false;
        boolean found = false;
        if (multithreaded) { 
            metadata.sharedLock();
        }
        for (Class c = table; c != null; c = c.getSuperclass()) { 
            Table t = (Table)tables.get(c);
            if (t != null) { 
                found = true;
                if (multithreaded) { 
                    t.extent.exclusiveLock();
                }
                if (t.extent.remove(record)) { 
                    Iterator iterator = t.indicesMap.values().iterator();
                    while (iterator.hasNext()) { 
                        FieldIndex index = (FieldIndex)iterator.next();
                        index.remove(record);
                    }
                    removed = true;
                }
            }
        }
        if (!found) { 
            throw new StorageError(StorageError.CLASS_NOT_FOUND, table.getName());
        }            
        if (removed) {
            record.deallocate();
        }
        return removed;
    }
        
    /**
     * Add new index to the table. If such index already exists this method does nothing.
     * @param table class corresponding to the table
     * @param key field of the class to be indexed
     * @param unique if index is unique or not
     * @exception StorageError (CLASS_NOT_FOUND) exception is thrown if there is no table corresponding to 
     * the specified class
     * @return <code>true</code> if index is created, <code>false</code> if index
     * already exists
     */
    public boolean createIndex(Class table, String key, boolean unique) { 
        Table t = locateTable(table, true);
        if (t.indicesMap.get(key) == null) { 
            FieldIndex index = storage.createFieldIndex(table, key, unique);
            t.indicesMap.put(key, index);
            t.indices.add(index);
            return true;
        }
        return false;
    }

    /**
     * Drop index for the specified table and key.
     * Does nothing if there is no such index.
     * @param table class corresponding to the table
     * @param key field of the class to be indexed
     * @exception StorageError (CLASS_NOT_FOUND) exception is thrown if there is no table corresponding to 
     * the specified class
     * @return <code>true</code> if index is deleted, <code>false</code> if index
     * is not found
     */
    public boolean dropIndex(Class table, String key) { 
        Table t = locateTable(table, true);
        FieldIndex index = (FieldIndex)t.indicesMap.remove(key);
        if (index != null) { 
            t.indices.remove(t.indices.indexOf(index));
            return true;
        }
        return false;
    }

    /**
     * Get indices for the specified table
     * @param table class corresponding to the table
     * @return map of table indices
     */
    public HashMap getIndices(Class table)
    {
        Table t = locateTable(table, true, false);
        return t == null ? new HashMap() : t.indicesMap;
    }

    /**
     * Exclude record from specified index. This method is needed to perform update of indexed
     * field (key). Before updating the record, it is necessary to exclude it from indices
     * which keys are affected. After updating the field, record should be reinserted in these indices
     * using includeInIndex method.<P>
     * If there is no table associated with class of this object, then 
     * database will search for table associated with superclass and so on...<P>
     * This method does nothing if there is no index for the specified field.
     * @param record object to be excluded from the specified index
     * @param key name of the indexed field
     * @exception StorageError (CLASS_NOT_FOUND) exception is thrown if there is no table corresponding to 
     * record class
     * @return <code>true</code> if record is excluded from index, <code>false</code> if 
     * there is no such index
     */
    public boolean excludeFromIndex(IPersistent record, String key) {
        return excludeFromIndex(record.getClass(), record, key);
    }

    /**
     * Exclude record from specified index. This method is needed to perform update of indexed
     * field (key). Before updating the record, it is necessary to exclude it from indices
     * which keys are affected. After updating the field, record should be reinserted in these indices
     * using includeInIndex method.<P>
     * If there is no table associated with class of this object, then 
     * database will search for table associated with superclass and so on...<P>
     * This method does nothing if there is no index for the specified field.
     * @param table class corresponding to the table
     * @param record object to be excluded from the specified index
     * @param key name of the indexed field
     * @exception StorageError (CLASS_NOT_FOUND) exception is thrown if there is no table corresponding to 
     * the specified class
     * @return <code>true</code> if record is excluded from index, <code>false</code> if 
     * there is no such index
     */
    public boolean excludeFromIndex(Class table, IPersistent record, String key) {
        Table t = locateTable(table, true);
        FieldIndex index = (FieldIndex)t.indicesMap.get(key);
        if (index != null) { 
            index.remove(record);
            return true;
        }
        return false;
    }


    /**
     * Include record in the specified index. This method is needed to perform update of indexed
     * field (key). Before updating the record, it is necessary to exclude it from indices
     * which keys are affected using excludeFromIndex method. After updating the field, record should be 
     * reinserted in these indices using this method.<P>
     * If there is no table associated with class of this object, then 
     * database will search for table associated with superclass and so on...<P>
     * This method does nothing if there is no index for the specified field.
     * @param record object to be excluded from the specified index
     * @param key name of the indexed field
     * @exception StorageError (CLASS_NOT_FOUND) exception is thrown if there is no table corresponding to 
     * the specified class
     * @return <code>true</code> if record is included in index, <code>false</code> if 
     * there is no such index
     */
    public boolean includeInIndex(IPersistent record, String key) { 
        return includeInIndex(record.getClass(), record, key);
    }

    /**
     * Include record in the specified index. This method is needed to perform update of indexed
     * field (key). Before updating the record, it is necessary to exclude it from indices
     * which keys are affected using excludeFromIndex method. After updating the field, record should be 
     * reinserted in these indices using this method.<P>
     * If there is no table associated with class of this object, then 
     * database will search for table associated with superclass and so on...<P>
     * This method does nothing if there is no index for the specified field.
     * @param table class corresponding to the table
     * @param record object to be excluded from the specified index
     * @param key name of the indexed field
     * @exception StorageError (CLASS_NOT_FOUND) exception is thrown if there is no table corresponding to 
     * the specified class
     * @return <code>true</code> if record is included in index, <code>false</code> if 
     * there is no such index
     */
    public boolean includeInIndex(Class table, IPersistent record, String key) { 
        Table t = locateTable(table, true);
        FieldIndex index = (FieldIndex)t.indicesMap.get(key);
        if (index != null) { 
            index.put(record);
            return true;
        }
        return false;
    }


    /**
     * Select record from specified table
     * @param table class corresponding to the table
     * @param predicate search predicate
     * @return iterator through selected records. This iterator doesn't support remove() method
     * @exception StorageError (CLASS_NOT_FOUND) exception is thrown if there is no table corresponding to 
     * the specified class
     * @exception CompileError exception is thrown if predicate is not valid JSQL exception
     * @exception JSQLRuntimeException exception is thrown if there is runtime error during query execution
     */
    public Iterator select(Class table, String predicate) { 
        return select(table, predicate, false);
    }

    /**
     * Select record from specified table
     * @param table class corresponding to the table
     * @param predicate search predicate
     * @param forUpdate <code>true</code> if records are selected for update - in this case exclusive lock is set 
     * for the table to avoid deadlock.
     * @return iterator through selected records. This iterator doesn't support remove() method
     * @exception StorageError (CLASS_NOT_FOUND) exception is thrown if there is no table corresponding to 
     * the specified class
     * @exception CompileError exception is thrown if predicate is not valid JSQL exception
     * @exception JSQLRuntimeException exception is thrown if there is runtime error during query execution
     */
    public Iterator select(Class table, String predicate, boolean forUpdate) { 
        Query q = prepare(table, predicate, forUpdate);
        return q.execute(getRecords(table));
    }

    /**
     * Prepare JSQL query. Prepare is needed for queries with parameters. Also
     * preparing query can improve speed if query will be executed multiple times
     * (using prepare, it is compiled only once).<P>
     * To execute prepared query, you should use Query.execute(db.getRecords(XYZ.class)) method
     * @param table class corresponding to the table
     * @param predicate search predicate
     * @exception StorageError (CLASS_NOT_FOUND) exception is thrown if there is no table corresponding to 
     * the specified class
     * @exception CompileError exception is thrown if predicate is not valid JSQL exception
     */
    public Query prepare(Class table, String predicate) {
        return prepare(table, predicate, false);
    }

    /**
     * Prepare JSQL query. Prepare is needed for queries with parameters. Also
     * preparing query can improve speed if query will be executed multiple times
     * (using prepare, it is compiled only once).<P>
     * To execute prepared query, you should use Query.execute(db.getRecords(XYZ.class)) method
     * @param table class corresponding to the table
     * @param predicate search predicate
     * @param forUpdate <code>true</code> if records are selected for update - in this case exclusive lock is set 
     * for the table to avoid deadlock.
     * @exception StorageError (CLASS_NOT_FOUND) exception is thrown if there is no table corresponding to 
     * the specified class
     * @exception CompileError exception is thrown if predicate is not valid JSQL exception
     */
    public Query prepare(Class table, String predicate, boolean forUpdate) { 
        Table t = locateTable(table, forUpdate, false);
        Query q = storage.createQuery();
        q.prepare(table, predicate);            
        while (t != null) { 
            Iterator iterator = t.indicesMap.entrySet().iterator();
            while (iterator.hasNext()) { 
                Map.Entry entry = (Map.Entry)iterator.next();
                FieldIndex index = (FieldIndex)entry.getValue();
                String key = (String)entry.getKey();
                q.addIndex(key, index);
            }
            t = locateTable(t.type.getSuperclass(), forUpdate, false);
        }
        return q;
    }
        
    /** 
     * Get iterator through all table records
     * @param table class corresponding to the table
     * @return iterator through all table records.
     * @exception StorageError (CLASS_NOT_FOUND) exception is thrown if there is no table corresponding to 
     * the specified class
     */
    public Iterator getRecords(Class table) { 
        return getRecords(table, false);
    }

    /** 
     * Get iterator through all table records
     * @param table class corresponding to the table
     * @param forUpdate <code>true</code> if records are selected for update - in this case exclusive lock is set 
     * for the table to avoid deadlock.
     * @return iterator through all table records.
     * @exception StorageError (CLASS_NOT_FOUND) exception is thrown if there is no table corresponding to 
     * the specified class
     */
    public Iterator getRecords(Class table, boolean forUpdate) { 
        Table t = locateTable(table, forUpdate, false);
        return t == null ? new ArrayList().iterator() : t.extent.iterator();
    }
    
    /**
     * Get storage associated with this database
     * @return underlying storage
     */
    public Storage getStorage() { 
        return storage;
    }

    static class Table extends Persistent { 
        IPersistentSet extent;
        Link           indices;

        transient HashMap indicesMap = new HashMap();
        transient Class type;

        public void onLoad() { 
            for (int i = indices.size(); --i >= 0;) { 
                FieldIndex index = (FieldIndex)indices.get(i);
                indicesMap.put(index.getKeyFields()[0].getName(), index);
            }
        }
    }                        

    HashMap tables;
    Storage storage;
    Index   metadata;
    boolean multithreaded;
}
