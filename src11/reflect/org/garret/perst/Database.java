package org.garret.perst;

import java.util.*;
import org.garret.perst.reflect.*;
import org.garret.perst.fulltext.*;

/**
 * This class emulates relational database on top of Perst storage
 * It maintain class extends, associated indices, prepare queries.
 */
public class Database implements IndexProvider { 
    /** 
     * Constructor of database. This method initialize database if it not initialized yet.
     * Starting from 2.72 version of Perst.Net, it supports automatic
     * creation of table descriptors when Database class is used.
     * So now it is not necessary to explicitly create tables and indices -
     * the Database class will create them itself on demand.
     * Indexable attribute should be used to mark key fields for which index should be created.
     * Table descriptor is created when instance of the correspondent class is first time stored 
     * in the database. Perst creates table descriptors for all derived classes up
     * to the root java.lang.Object class.
     * @param storage opened storage. Storage should be either empty (non-initialized, either
     * previously initialized by the this method. It is not possible to open storage with 
     * root object other than table index created by this constructor.
     * @param multithreaded <code>true</code> if database should support concurrent access
     * to the data from multiple threads.
     */
    public Database(Storage storage, boolean multithreaded) { 
        this(storage, multithreaded, true, null);
    }

    /** 
     * Constructor of database. This method initialize database if it not initialized yet.
     * @param storage opened storage. Storage should be either empty (non-initialized, either
     * previously initialized by the this method. It is not possible to open storage with 
     * root object other than table index created by this constructor.
     * @param multithreaded <code>true</code> if database should support concurrent access
     * to the data from multiple threads.
     * @param autoRegisterTables automatically create tables descriptors for instances of new 
     * @param helper helper for full text index
     * classes inserted in the database
     */
    public Database(Storage storage, boolean multithreaded, boolean autoRegisterTables, FullTextSearchHelper helper) { 
        this.storage = storage;
        this.multithreaded = multithreaded;
        this.autoRegisterTables = autoRegisterTables;
        if (multithreaded) { 
            storage.setProperty("perst.alternative.btree", Boolean.TRUE);
        }
        storage.setProperty("perst.concurrent.iterator", Boolean.TRUE);
        Object root = storage.getRoot();
        boolean schemaUpdated = false;
        if (root instanceof Index) { // backward compatibility
            beginTransaction();
            metadata = new Metadata(storage, (Index)root, helper);
            storage.setRoot(metadata);
            schemaUpdated = true;
        } else if (root == null) {
            beginTransaction();
            metadata = new Metadata(storage, helper);
            storage.setRoot(metadata);
            schemaUpdated = true;
        } else { 
            metadata = (Metadata)root;
        }
        reflection = ReflectionProvider.getInstance();
        schemaUpdated |= reloadSchema();
        if (schemaUpdated) {
            commitTransaction();
        }
    }

    private boolean reloadSchema() 
    {
        boolean schemaUpdated = false;
        metadata = (Metadata)storage.getRoot();
        Iterator iterator = metadata.metaclasses.entryIterator();
        tables = new Hashtable();
        while (iterator.hasNext()) { 
            Map.Entry map = (Map.Entry)iterator.next();
            Table table = (Table)map.getValue();
            Type cls = reflection.getType((String)map.getKey());
            if (cls == null) { 
                throw new StorageError(StorageError.CLASS_NOT_FOUND);
            }
            table.setClass(cls);
            tables.put(cls, table);
            schemaUpdated |= addIndices(table, cls);
        }
        return schemaUpdated;
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
     * Enable or disable automatic creation of indices. 
     * If this feature is enabled, Perst will try to create new index each time when it needs it during
     * query execution
     * @param enabled if true, then automatic creation of indices is enabled
     * @return previous status
     */
    public boolean enableAutoIndices(boolean enabled) { 
        boolean prev = autoIndices;
        autoIndices = enabled;
        return prev;
    }

    /**
     * Begin transaction
     */
    public void beginTransaction() { 
        if (multithreaded) { 
            storage.beginSerializableTransaction();
        }
    }

    /**
     * Commit transaction
     */
    public void commitTransaction() { 
        if (multithreaded) { 
            storage.commitSerializableTransaction();
        } else { 
            storage.commit();
        }
    }
    
    private void checkTransaction() {
        if (!storage.isInsideThreadTransaction()) { 
            throw new StorageError(StorageError.NOT_IN_TRANSACTION);
        }
    }

    /**
     * Rollback transaction
     */
    public void rollbackTransaction() { 
        if (multithreaded) { 
            storage.rollbackSerializableTransaction();
        } else { 
            storage.rollback();
        }
        reloadSchema();
    }

    private Type getType(Class table) { 
        Type type = reflection.getType(table);
        if (type == null) { 
            throw new StorageError(StorageError.CLASS_NOT_FOUND);
        }
        return type;
    }
    
    /**
     * Create table for the specified class.
     * This function does nothing if table for such class already exists
     * @param table class corresponding to the table
     * @return <code>true</code> if table is created, <code>false</code> if table 
     * alreay exists
     * @deprecated Since version 2.75 of Perst it is not necessary to create table and index 
     * descriptors explicitly: them are automatically create when object is inserted in the 
     * database first time (to mark fields for which indices should be created, use Indexable 
     * annotation)
     */
    public boolean createTable(Class table) { 
        if (multithreaded) { 
            checkTransaction();
            metadata.exclusiveLock();
        }
        Type type = getType(table);
        if (tables.get(type) == null) { 
            Table t = new Table();
            t.extent = storage.createSet();
            t.indices = storage.createLink();
            t.indicesMap = new Hashtable();
            t.setClass(type);
            tables.put(type, t);
            metadata.metaclasses.put(table.getName(), t);
            addIndices(t, type);
            return true;
        }
        return false;
    }
               
    private boolean addIndices(Table table, Type type) {
        boolean schemaUpdated = false;
        Field[] fields = type.getDeclaredFields();
        for (int i = 0; i < fields.length; i++) { 
            Field f = fields[i];
            int modifiers = f.getModifiers();
            if ((modifiers & Modifier.Indexable) != 0) { 
                schemaUpdated |= createIndex(table, type.getDescribedClass(), f.getName(), (modifiers & Modifier.Unique) != 0, (modifiers & Modifier.CaseInsensitive) != 0, (modifiers & Modifier.RandomAccess) != 0);
            }            
        }
        return schemaUpdated;
    }

    /**
     * Drop table associated with this class. Do nothing if there is no such table in the database.
     * @param table class corresponding to the table
     * @return <code>true</code> if table is deleted, <code>false</code> if table 
     * is not found
     */
    public boolean dropTable(Class table) { 
        if (multithreaded) { 
            checkTransaction();
            metadata.exclusiveLock();
        }
        Type type = getType(table);
        Table t = (Table)tables.remove(type);
        if (t != null) { 
            Iterator i = t.extent.iterator();
            while (i.hasNext()) { 
                IPersistent obj = (IPersistent)i.next();
                if (obj instanceof FullTextSearchable || t.fullTextIndexableFields.length != 0) { 
                    metadata.fullTextIndex.delete(obj);
                }
                Class baseClass;
                for (Type baseType = type; (baseClass = baseType.getSuperclass()) != Persistent.class;)  {
                    baseType = getType(baseClass);
                    Table baseTable = (Table)tables.get(baseType);
                    if (baseTable != null) { 
                        if (multithreaded) { 
                            baseTable.extent.exclusiveLock();
                        }
                        if (baseTable.extent.remove(obj)) { 
                            Enumeration iterator = t.indicesMap.elements();
                            while (iterator.hasMoreElements()) { 
                                FieldIndex index = (FieldIndex)iterator.nextElement();
                                index.remove(obj);
                            }
                        }
                    }
                }    
                obj.deallocate();
            }
            metadata.metaclasses.remove(table.getName());
            t.deallocate();
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
     * @exception StorageError(CLASS_NOT_FOUND) exception is thrown if there is no table corresponding to 
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
        Type type;
        if (multithreaded) { 
            checkTransaction();
            metadata.sharedLock();
        }
        if (!autoRegisterTables) { 
            for (Class c = cls; c != Persistent.class && (table = (Table)tables.get(type = getType(c))) == null; c = type.getSuperclass());
        } else { 
            table = (Table)tables.get(type = getType(cls));
        }
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

    private void registerTable(Class cls) { 
        if (multithreaded) { 
            checkTransaction();
            metadata.sharedLock();
        }
        if (autoRegisterTables) { 
            boolean exclusiveLockSet = false;    
            Type type;
            for (Class c = cls; c != Persistent.class; c = type.getSuperclass()) { 
                type = getType(c);
                Table t = (Table)tables.get(type);
                if (t == null) { 
                    if (!exclusiveLockSet) { 
                        metadata.unlock(); // try to avoid deadlock caused by concurrent insertion of objects
                        exclusiveLockSet = true;
                    }
                    createTable(c);
                }
            }
        }
    }
        
    /**
     * Update full text index for modified record
     * @param record updated record
     */
    public void updateFullTextIndex(IPersistent record) 
    { 
        if (multithreaded) { 
            checkTransaction();
            metadata.fullTextIndex.exclusiveLock();
        }
        if (record instanceof FullTextSearchable) { 
            metadata.fullTextIndex.add((FullTextSearchable)record);
        } else { 
            StringBuffer fullText = new StringBuffer();
            Type type;
            for (Class c = record.getClass(); c != Persistent.class; c = type.getSuperclass()) { 
                type = getType(c);
                Table t = (Table)tables.get(type);
                if (t != null) { 
                    Field[] fields = t.fullTextIndexableFields;
                    for (int i = 0; i < fields.length; i++) { 
                        Field f = fields[i];
                        Object text = f.get(record);
                        if (text != null) { 
                            fullText.append(' ');
                            fullText.append(text.toString());
                        }
                    }
                }
            }
            metadata.fullTextIndex.add(record, new StringReader(fullText.toString()), null);
        }
    }

    /**
     * Add new record to the specified table. Record is inserted in table corresponding to the specified class.
     * Record will be automatically added to all indices existed for this table.
     * @param table class corresponding to the table
     * @param record object to be inserted in the table
     * @return <code>true</code> if record was successfully added to the table, <code>false</code>
     * if there is already such record (object with the same ID) in the table or there is some record with the same value of 
     * unique key field
     * @exception StorageError(CLASS_NOT_FOUND) exception is thrown if there is no table corresponding to 
     * record class
     */
    public boolean addRecord(Class table, IPersistent record) { 
        boolean added = false;
        boolean found = false;
        registerTable(table);
        Vector wasInsertedIn = new Vector();
        StringBuffer fullText = new StringBuffer();
        Type type;
        for (Class c = table; c != Persistent.class; c = type.getSuperclass()) { 
            type = getType(c);
            Table t = (Table)tables.get(type);
            if (t != null) { 
                found = true;
                if (multithreaded) { 
                    t.extent.exclusiveLock();
                }
                if (t.extent.add(record)) { 
                    wasInsertedIn.addElement(t.extent);
                    Enumeration iterator = t.indicesMap.elements();
                    while (iterator.hasMoreElements()) { 
                        FieldIndex index = (FieldIndex)iterator.nextElement();
                        if (index.put(record)) {
                            wasInsertedIn.addElement(index);
                        } else if (index.isUnique()) { 
                            iterator = wasInsertedIn.elements();
                            while (iterator.hasMoreElements()) { 
                                Object idx = iterator.nextElement();
                                if (idx instanceof IPersistentSet) {
                                    ((IPersistentSet)idx).remove(record);
                                } else { 
                                    ((FieldIndex)idx).remove(record);
                                }
                            }
                            return false;
                        }
                    }
                    Field[] fields = t.fullTextIndexableFields;
                    for (int i = 0; i < fields.length; i++) { 
                        Field f = fields[i];
                        Object text = f.get(record);
                        if (text != null) { 
                            fullText.append(' ');
                            fullText.append(text.toString());
                        }
                    }
                    added = true;
                }
            }
        }
        if (!found) { 
            throw new StorageError(StorageError.CLASS_NOT_FOUND, table.getName());
        }      
        if (record instanceof FullTextSearchable || fullText.length() != 0) { 
            if (multithreaded) { 
                metadata.fullTextIndex.exclusiveLock();
            }
            if (record instanceof FullTextSearchable) { 
                metadata.fullTextIndex.add((FullTextSearchable)record);
            } else { 
                metadata.fullTextIndex.add(record, new StringReader(fullText.toString()), null);
            }
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
     * @exception StorageError(CLASS_NOT_FOUND) exception is thrown if there is no table corresponding to 
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
     * @exception StorageError(CLASS_NOT_FOUND) exception is thrown if there is no table corresponding to 
     * specified class
     */
    public boolean deleteRecord(Class table, IPersistent record) { 
        boolean removed = false;
        if (multithreaded) { 
            checkTransaction();
            metadata.sharedLock();
        }
        boolean fullTextIndexed = false;
        Type type;
        for (Class c = table; c != Persistent.class; c = type.getSuperclass()) { 
            type = getType(c);
            Table t = (Table)tables.get(type);
            if (t != null) { 
                if (multithreaded) { 
                    t.extent.exclusiveLock();
                }
                if (t.extent.remove(record)) { 
                    Enumeration iterator = t.indicesMap.elements();
                    while (iterator.hasMoreElements()) { 
                        FieldIndex index = (FieldIndex)iterator.nextElement();
                        index.remove(record);
                    }
                    if (t.fullTextIndexableFields.length != 0) { 
                        fullTextIndexed = true;
                    }
                    removed = true;
                }
            }
        }
        if (removed) {
            if (record instanceof FullTextSearchable || fullTextIndexed) {
                if (multithreaded) { 
                    metadata.fullTextIndex.exclusiveLock();
                }
                metadata.fullTextIndex.delete(record);
            }
            record.deallocate();
        }
        return removed;
    }
        
    /**
     * Add new index to the table. If such index already exists this method does nothing.
     * @param table class corresponding to the table
     * @param key field of the class to be indexed
     * @param unique if index is unique or not
     * @exception StorageError(CLASS_NOT_FOUND) exception is thrown if there is no table corresponding to 
     * the specified class
     * @return <code>true</code> if index is created, <code>false</code> if index
     * already exists
     * @deprecated since version 2.75 of Perst it is not necessary to create table and index 
     * descriptors explicitly: them are automatically create when object is inserted in the 
     * database first time (to mark fields for which indices should be created, use Indexable 
     * annotation)
     */
    public boolean createIndex(Class table, String key, boolean unique) { 
        return createIndex(locateTable(table, true), table, key, unique, false, false);
    }
    /**
     * Add new index to the table. If such index already exists this method does nothing.
     * @param table class corresponding to the table
     * @param key field of the class to be indexed
     * @param unique if index is unique or not
     * @param caseInsensitive if string index is case insensitive
     * @exception StorageError(CLASS_NOT_FOUND) exception is thrown if there is no table corresponding to 
     * the specified class
     * @return <code>true</code> if index is created, <code>false</code> if index
     * already exists
     * @deprecated since version 2.75 of Perst it is not necessary to create table and index 
     * descriptors explicitly: them are automatically cerate when objct is inserted in the 
     * database first time (to mark fields for which indices should be created, use Indexable 
     * annotaion)
     */
    public boolean createIndex(Class table, String key, boolean unique, boolean caseInsensitive) { 
        return createIndex(locateTable(table, true), table, key, unique, caseInsensitive, false);
    }

    /**
     * Add new index to the table. If such index already exists this method does nothing.
     * @param table class corresponding to the table
     * @param key field of the class to be indexed
     * @param unique if index is unique or not
     * @param caseInsensitive if string index is case insensitive
     * @param randomAccess index should support fast access to elements by position
     * @exception StorageError(CLASS_NOT_FOUND) exception is thrown if there is no table corresponding to 
     * the specified class
     * @return <code>true</code> if index is created, <code>false</code> if index
     * already exists
     * @deprecated since version 2.75 of Perst it is not necessary to create table and index 
     * descriptors explicitly: them are automatically cerate when objct is inserted in the 
     * database first time (to mark fields for which indices should be created, use Indexable 
     * annotaion)
     */
    public boolean createIndex(Class table, String key, boolean unique, boolean caseInsensitive, boolean randomAccess) { 
        return createIndex(locateTable(table, true), table, key, unique, caseInsensitive, randomAccess);
    }

    private boolean createIndex(Table t, Class c, String key, boolean unique, boolean caseInsensitive, boolean randomAccess) { 
        if (t.indicesMap.get(key) == null) { 
            FieldIndex index = randomAccess 
                ? storage.createRandomAccessFieldIndex(c, key, unique, caseInsensitive) 
                : storage.createFieldIndex(c, key, unique, caseInsensitive);
            t.indicesMap.put(key, index);
            t.indices.add(index);
            Iterator i = t.extent.iterator(); 
            while (i.hasNext()) { 
                IPersistent obj = (IPersistent)i.next();
                if (!index.put(obj) && index.isUnique()) { 
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    /**
     * Drop index for the specified table and key.
     * Does nothing if there is no such index.
     * @param table class corresponding to the table
     * @param key field of the class to be indexed
     * @exception StorageError(CLASS_NOT_FOUND) exception is thrown if there is no table corresponding to 
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
     * Get index for the specified field of the class
     * @param table class where index is located
     * @param key field of the class
     * @return Index for this field or null if index doesn't exist
     */
    public GenericIndex getIndex(Class table, String key)
    {
        Table t = locateTable(table, false, false);
        if (t != null) { 
            synchronized (t.indicesMap) { 
                GenericIndex index = (GenericIndex)t.indicesMap.get(key);
                if (index == null && autoIndices && key.indexOf('.') < 0) { 
                    StorageListener listener = storage.getListener();
                    if (listener != null) { 
                        listener.indexCreated(table, key);
                    }
                    createIndex(t, table, key, false, false, false); 
                    index = (GenericIndex)t.indicesMap.get(key);
                }
                return index;
            }
        }
        return null;
    }

    /**
     * Get indices for the specified table
     * @param table class corresponding to the table
     * @return map of table indices
     */
    public Hashtable getIndices(Class table)
    {
        Table t = locateTable(table, true, false);
        return t == null ? new Hashtable() : t.indicesMap;
    }

    /**
     * Exclude record from all indices. This method is needed to perform update of indexed
     * field (key). Before updating the record, it is necessary to exclude it from indices
     * which keys are affected. After updating the field, record should be reinserted in these indices
     * using includeInIndex method. If your know which fields will be updated and which indices
     * exist for this table, it is more efficient to use excludeFromIndex method to exclude
     * object only from affected indices.<P>
     * @param record object to be excluded from the specified index
     * @exception StorageError(CLASS_NOT_FOUND) exception is thrown if there is no table corresponding to 
     * record class
     */
    public void excludeFromAllIndices(IPersistent record) {
        excludeFromAllIndices(record.getClass(), record);
    }

    /**
     * Exclude record from all indices. This method is needed to perform update of indexed
     * field (key). Before updating the record, it is necessary to exclude it from indices
     * which keys are affected. After updating the field, record should be reinserted in these indices
     * using includeInIndex method. If your know which fields will be updated and which indices
     * exist for this table, it is more efficient to use excludeFromIndex method to exclude
     * object only from affected indices.<P>
     * @param table class corresponding to the table
     * @param record object to be excluded from the specified index
     * @exception StorageError(CLASS_NOT_FOUND) exception is thrown if there is no table corresponding to 
     * record class
     */
    public void excludeFromAllIndices(Class table, IPersistent record) {
        if (multithreaded) { 
            checkTransaction();
            metadata.sharedLock();
        }
        boolean fullTextIndexed = false;
        Type type;
        for (Class c = table; c != Persistent.class; c = type.getSuperclass()) { 
            type = getType(c);
            Table t = (Table)tables.get(type);
            if (t != null) { 
                if (multithreaded) { 
                    t.extent.exclusiveLock();
                }
                Enumeration iterator = t.indicesMap.elements();
                while (iterator.hasMoreElements()) { 
                    FieldIndex index = (FieldIndex)iterator.nextElement();
                    index.remove(record);
                }
                if (t.fullTextIndexableFields.length != 0) { 
                    fullTextIndexed = true;
                }
            }
        }
        if (record instanceof FullTextSearchable || fullTextIndexed) {
            if (multithreaded) { 
                metadata.fullTextIndex.exclusiveLock();
            }
            metadata.fullTextIndex.delete(record);
        }
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
     * @exception StorageError(CLASS_NOT_FOUND) exception is thrown if there is no table corresponding to 
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
     * @exception StorageError(CLASS_NOT_FOUND) exception is thrown if there is no table corresponding to 
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
     * Include record in all indices. This method is needed to perform update of indexed
     * fields (keys). Before updating the record, it is necessary to exclude it from indices
     * which keys are affected using excludeFromIndices method. After updating the field, record should be 
     * reinserted in these indices using this method. If your know which fields will be updated and which indices
     * exist for this table, it is more efficient to use excludeFromIndex/includeInIndex methods to touch
     * only affected indices.
     * @param record object to be excluded from the specified index
     * @exception StorageError(CLASS_NOT_FOUND) exception is thrown if there is no table corresponding to 
     * the specified class
     * @return <code>true</code> if record is included in indices, <code>false</code> if 
     * there is no such index or unique constraint is violated
     */
    public boolean includeInAllIndices(IPersistent record) { 
        return includeInAllIndices(record.getClass(), record);
    }

    /**
     * Include record in all indices. This method is needed to perform update of indexed
     * fields (keys). Before updating the record, it is necessary to exclude it from indices
     * which keys are affected using excludeFromIndices method. After updating the field, record should be 
     * reinserted in these indices using this method. If your know which fields will be updated and which indices
     * exist for this table, it is more efficient to use excludeFromIndex/includeInIndex methods to touch
     * only affected indices.
     * @param table class corresponding to the table
     * @param record object to be excluded from the specified index
     * @return <code>true</code> if record is included in index, <code>false</code> if 
     * there is no such index or unique constraint is violated
     * @exception StorageError(CLASS_NOT_FOUND) exception is thrown if there is no table corresponding to 
     * the specified class
     * @return <code>true</code> if record is included in index, <code>false</code> if 
     * there is no such index or unique constraint is violated
     */
    public boolean includeInAllIndices(Class table, IPersistent record) { 
        if (multithreaded) { 
            checkTransaction();
        }
        Vector wasInsertedIn = new Vector();
        StringBuffer fullText = new StringBuffer();
        Type type;
        for (Class c = table; c != Persistent.class; c = type.getSuperclass()) { 
            type = getType(c);
            Table t = (Table)tables.get(type);
            if (t != null) { 
                if (multithreaded) { 
                    t.extent.exclusiveLock();
                }
                Enumeration iterator = t.indicesMap.elements();
                while (iterator.hasMoreElements()) { 
                    FieldIndex index = (FieldIndex)iterator.nextElement();
                    if (index.put(record)) {
                        wasInsertedIn.addElement(index);
                    } else if (index.isUnique()) { 
                        iterator = wasInsertedIn.elements();
                        while (iterator.hasMoreElements()) { 
                            Object idx = iterator.nextElement();
                            if (idx instanceof IPersistentSet) {
                                ((IPersistentSet)idx).remove(record);
                            } else { 
                                ((FieldIndex)idx).remove(record);
                            }
                        }
                        return false;
                    }
                }
                Field[] fields = t.fullTextIndexableFields;
                for (int i = 0; i < fields.length; i++) { 
                    Field f = fields[i];
                    Object text = f.get(record);
                    if (text != null) { 
                        fullText.append(' ');
                        fullText.append(text.toString());
                    }
                }
            }
        }
        if (record instanceof FullTextSearchable || fullText.length() != 0) { 
            if (multithreaded) { 
                metadata.fullTextIndex.exclusiveLock();
            }
            if (record instanceof FullTextSearchable) { 
                metadata.fullTextIndex.add((FullTextSearchable)record);
            } else { 
                metadata.fullTextIndex.add(record, new StringReader(fullText.toString()), null);
            }
        }
        return true;
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
     * @exception StorageError(CLASS_NOT_FOUND) exception is thrown if there is no table corresponding to 
     * the specified class
     * @return <code>true</code> if record is included in index, <code>false</code> if 
     * there is no such index or unique constraint is violated
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
     * @exception StorageError(CLASS_NOT_FOUND) exception is thrown if there is no table corresponding to 
     * the specified class
     * @return <code>true</code> if record is included in index, <code>false</code> if 
     * there is no such index or unique constraint is violated
     */
    public boolean includeInIndex(Class table, IPersistent record, String key) { 
        Table t = locateTable(table, true);
        FieldIndex index = (FieldIndex)t.indicesMap.get(key);
        if (index != null) { 
            return index.put(record) || !index.isUnique();
        }
        return false;
    }

    /**
     * This method can be used to update a key field. It is responsibility of programmer in Perst
     * to maintain consistency of indices: before updating key field it is necessary to exclude 
     * the object from the index and after assigning new value to the key field - reinsert it in the index.
     * It can be done using excludeFromIndex/includeInIndex methods, but updateKey combines all this steps:
     * exclude from index, update, mark object as been modified and reinsert in index.
     * It is safe to call updateKey method for fields which are actually not used in any index - 
     * in this case excludeFromIndex/includeInIndex do nothing.
     * @param record updated object
     * @param key name of the indexed field
     * @param value new value of indexed field
     * @exception StorageError(INDEXED_FIELD_NOT_FOUND) exception is thrown if specified field is not found in 
     * @exception StorageError(ACCESS_VIOLATION) exception is thrown if it is not possible to change field value
     */ 
    public void updateKey(IPersistent record, String key, Object value) {
        updateKey(record.getClass(), record, key, value);
    }
            

    /**
     * This method can be used to update a key field. It is responsibility of programmer in Perst
     * to maintain consistency of indices: before updating key field it is necessary to exclude 
     * the object from the index and after assigning new value to the key field - reinsert it in the index.
     * It can be done using excludeFromIndex/includeInIndex methods, but updateKey combines all this steps:
     * exclude from index, update, mark object as been modified and reinsert in index.
     * It is safe to call updateKey method for fields which are actually not used in any index - 
     * in this case excludeFromIndex/includeInIndex do nothing.
     * @param table class corresponding to the table
     * @param record updated object
     * @param key name of the indexed field
     * @param value new value of indexed field
     * @exception StorageError(INDEXED_FIELD_NOT_FOUND) exception is thrown if specified field is not found in the class
     * @exception StorageError(ACCESS_VIOLATION) exception is thrown if it is not possible to change field value
     */ 
    public void updateKey(Class table, IPersistent record, String key, Object value) {
        excludeFromIndex(table, record, key);
        Type type = getType(table);
        Field f = type.getField(key);
        if (f == null) { 
            throw new StorageError(StorageError.INDEXED_FIELD_NOT_FOUND, table.getName());
        } 
        f.set(record, value);
        record.modify();
        includeInIndex(table, record, key);
    }
            


    /**
     * Select record from specified table
     * @param table class corresponding to the table
     * @param predicate search predicate
     * @exception StorageError(CLASS_NOT_FOUND) exception is thrown if there is no table corresponding to 
     * the specified class
     * @return iterator through selected records. This iterator doesn't support remove() method
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
     * @exception StorageError(CLASS_NOT_FOUND) exception is thrown if there is no table corresponding to 
     * the specified class
     * @return iterator through selected records. This iterator doesn't support remove() method
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
     * @exception StorageError(CLASS_NOT_FOUND) exception is thrown if there is no table corresponding to 
     * the specified class
     * @exception CompileError exception is thrown if predicate is not valid JSQL exception
     * @return prepared query
     */
    public Query prepare(Class table, String predicate) { 
        return prepare(table, predicate, false);
    }

    /**
     * Prepare JSQL query. Prepare is needed for queries with parameters. Also
     * preparing query can improve speed if query will be executed multiple times
     * (using prepare, it is compiled only once).<P>
     * To execute prepared query, you should use Query.execute() or 
     * Query.execute(db.getRecords(XYZ.class)) method
     * @param table class corresponding to the table
     * @param predicate search predicate
     * @param forUpdate <code>true</code> if records are selected for update - in this case exclusive lock is set 
     * for the table to avoid deadlock.
     * @exception StorageError(CLASS_NOT_FOUND) exception is thrown if there is no table corresponding to 
     * the specified class
     * @exception CompileError exception is thrown if predicate is not valid JSQL exception
     * @return prepared query
     */
    public Query prepare(Class table, String predicate, boolean forUpdate) { 
        Query q = createQuery(table, forUpdate);
        q.prepare(table, predicate);      
        return q;
    }
        
    /**
     * Create query for the specified class. You can use Query.getCodeGenerator method to generate 
     * query code.
     * @param table class corresponding to the table
     * @exception StorageError(CLASS_NOT_FOUND) exception is thrown if there is no table corresponding to 
     * the specified class
     * @return query without predicate
     */
    public Query createQuery(Class table)
    {
        return createQuery(table, false);
    }

    /**
     * Create query for the specified class. You can use Query.getCodeGenerator method to generate 
     * query code.
     * @param table class corresponding to the table
     * @param forUpdate <code>true</code> if records are selected for update - in this case exclusive lock is set 
     * for the table to avoid deadlock.
     * @exception StorageError(CLASS_NOT_FOUND) exception is thrown if there is no table corresponding to 
     * the specified class
     * @return query without predicate
     */
    public Query createQuery(Class table, boolean forUpdate) { 
        Table t = locateTable(table, forUpdate, false);
        Query q = storage.createQuery();
        q.setIndexProvider(this);
        q.setClass(table);
        while (t != null) { 
            q.setClassExtent(t.extent, multithreaded ? forUpdate ? Query.ClassExtentLockType.Exclusive : Query.ClassExtentLockType.Shared : Query.ClassExtentLockType.None);
            Enumeration iterator = t.indicesMap.keys();
            while (iterator.hasMoreElements()) { 
                String key = (String)iterator.nextElement();
                FieldIndex index = (FieldIndex)t.indicesMap.get(key);
                q.addIndex(key, index);
            }
            Class superclass = t.type.getSuperclass();
            if (superclass == Persistent.class) { 
                break;
            }
            t = locateTable(superclass, forUpdate, false);
        }
        return q;
    }

    /** 
     * Get iterator through all table records
     * @param table class corresponding to the table
     * @return iterator through all table records.
     * @exception StorageError(CLASS_NOT_FOUND) exception is thrown if there is no table corresponding to 
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
     * @exception StorageError(CLASS_NOT_FOUND) exception is thrown if there is no table corresponding to 
     * the specified class
     */
    public Iterator getRecords(Class table, boolean forUpdate) { 
        Table t = locateTable(table, forUpdate, false);
        return t == null ? new ArrayList().iterator() : new ClassFilterIterator(table, t.extent.iterator());
    }

    /**
     * Get number of records in the table
     * @param table class corresponding to the table
     * @return number of records in the table associated with specified class
     */
    public int countRecords(Class table) {
        return countRecords(table, false);
    }

    /**
     * Get number of records in the table 
     * @param table class corresponding to the table
     * @param forUpdate <code>true</code> if you are going to update the table - in this case exclusive lock is set 
     * for the table to avoid deadlock.
     * @return number of records in the table associated with specified class
     */
    public int countRecords(Class table, boolean forUpdate) { 
        Table t = locateTable(table, forUpdate, false);
        return t == null ? 0 : t.extent.size();        
    }

    /**
     * Get storage associated with this database
     * @return underlying storage
     */
    public Storage getStorage() { 
        return storage;
    }

    /**
     * Locate all documents containing words started with specified prefix
     * @param prefix word prefix
     * @param maxResults maximal amount of selected documents
     * @param timeLimit limit for query execution time
     * @param sort whether it is necessary to sort result by rank
     * @return result of query execution ordered by rank (of sort==true) or null in case of empty or incorrect query  
     */
    public FullTextSearchResult searchPrefix(String prefix, int maxResults, int timeLimit, boolean sort) {
        if (multithreaded) { 
            checkTransaction();
            metadata.fullTextIndex.sharedLock();
        }
        return metadata.fullTextIndex.searchPrefix(prefix, maxResults, timeLimit, sort);
    }

    /**
     * Get iterator through full text index keywords started with specified prefix
     * @param prefix keyword prefix (user empty string to get list of all keywords)
     * @return iterator through list of all keywords with specified prefix
     */
    public Iterator getKeywords(String prefix) {
        if (multithreaded) { 
            checkTransaction();
            metadata.fullTextIndex.sharedLock();
        }
        return metadata.fullTextIndex.getKeywords(prefix);
    }

    /**
     * Parse and execute full text search query
     * @param query text of the query
     * @param language language if the query
     * @param maxResults maximal amount of selected documents
     * @param timeLimit limit for query execution time
     * @return result of query execution ordered by rank or null in case of empty or incorrect query
     */
    public FullTextSearchResult search(String query, String language, int maxResults, int timeLimit) {
        if (multithreaded) { 
            checkTransaction();
            metadata.fullTextIndex.sharedLock();
        }
        return metadata.fullTextIndex.search(query, language, maxResults, timeLimit);
    }

    /**
     * Execute full text search query
     * @param query prepared query
     * @param maxResults maximal amount of selected documents
     * @param timeLimit limit for query execution time
     * @return result of query execution ordered by rank or null in case of empty or incorrect query
     */    
    public FullTextSearchResult search(FullTextQuery query, int maxResults, int timeLimit) {
        if (multithreaded) { 
            checkTransaction();
            metadata.fullTextIndex.sharedLock();
        }
        return metadata.fullTextIndex.search(query, maxResults, timeLimit);
    }


    public static class Metadata extends PersistentResource {
        Index metaclasses;
        FullTextIndex fullTextIndex;

        public void writeObject(IOutputStream out) {
            out.writeObject(metaclasses);
            out.writeObject(fullTextIndex);
        }

        public void readObject(IInputStream in) {
            metaclasses = (Index)in.readObject();
            fullTextIndex = (FullTextIndex)in.readObject();
        }

        Metadata(Storage storage, Index index, FullTextSearchHelper helper) { 
            super(storage);
            metaclasses = index;
            fullTextIndex = (helper != null) 
                ? FullTextIndexFactory.createFullTextIndex(storage, helper)
                : FullTextIndexFactory.createFullTextIndex(storage);
        }

        Metadata(Storage storage, FullTextSearchHelper helper) { 
            super(storage);
            metaclasses = storage.createIndex(Types.String, true);
            fullTextIndex = (helper != null) 
                ? FullTextIndexFactory.createFullTextIndex(storage, helper)
                : FullTextIndexFactory.createFullTextIndex(storage);
        }

        public Metadata() {}
    }
        
    public static class Table extends Persistent { 
        IPersistentSet extent;
        Link           indices;

        public void writeObject(IOutputStream out) {
            out.writeObject(extent);
            out.writeLink(indices);
        }

        public void readObject(IInputStream in) {
            extent = (IPersistentSet)in.readObject();
            indices = in.readLink();
            if (indices != null) { 
                for (int i = indices.size(); --i >= 0;) { 
                    FieldIndex index = (FieldIndex)indices.get(i);
                    indicesMap.put(index.getKeyFields()[0], index);
                }
            }
        }

        public void deallocate() {
            extent.deallocate();
            Enumeration e = indicesMap.elements(); 
            while (e.hasMoreElements()) { 
                ((FieldIndex)e.nextElement()).deallocate();
            }
            super.deallocate();
        }

        transient Hashtable indicesMap = new Hashtable();
        transient Field[] fullTextIndexableFields;
        transient Type type;

        void setClass(Type type) {
            this.type = type;
            ArrayList list = new ArrayList();
            Field[] fields = type.getDeclaredFields();
            for (int i = 0; i < fields.length; i++) { 
                Field f = fields[i];
                if ((f.getModifiers() & Modifier.FullTextIndexable) != 0) { 
                    list.add(f);
                }            
            }
            fullTextIndexableFields = new Field[list.size()];
            list.toArray(fullTextIndexableFields);
        } 
    }                        

    Hashtable tables;
    Storage   storage;
    Metadata  metadata;
    boolean   multithreaded;
    boolean   autoRegisterTables;
    boolean   autoIndices;
    ReflectionProvider reflection;
}

class ClassFilterIterator extends Iterator
{
    public int nextOid()
    {
        Object curr = obj;
        if (curr == null) { 
           throw new NoSuchElementException();
        }        
        moveNext();
        return ((Persistent)curr).getOid();
    }

    public boolean hasNext() {
        return obj != null;
    }

    public Object next() {
        Object curr = obj;
        if (curr == null) { 
           throw new NoSuchElementException();
        }        
        moveNext();
        return curr;
    }

    public ClassFilterIterator(Class c, Iterator i) {
        cls = c;
        iterator = i;
        moveNext();
   }
        
    public void remove() {
        throw new org.garret.perst.UnsupportedOperationException();
    }

    private void moveNext() {
        obj = null;
        while (iterator.hasNext()) { 
            Object curr = iterator.next();
            if (cls.isInstance(curr)) { 
                obj = curr;
                return;
            }
        }
    }

    private Iterator iterator;
    private Class cls;
    private Object obj;
}
