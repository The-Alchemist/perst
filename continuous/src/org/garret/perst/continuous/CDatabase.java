package org.garret.perst.continuous;

import java.util.*;
import java.io.*;
import org.garret.perst.*;
import org.garret.perst.impl.ClassDescriptor;

import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NoLockFactory;
import org.apache.lucene.queryParser.*;
import org.apache.lucene.search.Hits;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

/**
 * <p>
 * This class emulates relational database on top of Perst storage
 * It maintains class extends and associated indices. Is supports JSQL and full text search queries.
 * This class provides version control system, optimistic access control and full text search (using Lucene).
 * </p><p>
 * All transactions are isolated from each other and do not see changes made by other transactions 
 * (even committed transactions if commit happens after start of this transaction).
 * The database keeps all versions of the object hich are grouped in version history.
 * Version history is linear which means that no branches in version tree are possible. 
 * When transaction tries to update some object then working copy of the current version is created.
 * Only this copy is changed, leaving all other versions unchanged. This modification will be visible only
 * for the current transaction. Created or modified objects are linked in indices when transaction is committed.
 * It means that application will not be able to find in the index just inserted or updated object. It has 
 * first to commit the current transaction. All working copies created during transaction execution
 * are linked in he list stored in transaction context which is in turn associated with the current thread. 
 * It means two things:
 * <ol>
 * <li>one thread can participate in only one transaction and one transaction can be controlled only by one thread</li>
 * <li>size of transaction is limited by the amount of memory available for the application</li>
 * </ol>
 * When transaction is committed, then all its working copies are inspected. If the last version in version history is 
 * not equal to one from which working copy was created then conflict is detected and ConflictException is thrown.
 * And if insertion of working copy in index will cause unique constraint violation then NotUniqueException is thrown.
 * If no conflicts are detected, then transaction is committed and each working copy becomes new version in correspondent 
 * version history.
 * </p><p>
 * Isolation of transaction provided by CDatabase class is based on multiversioning and modification of working copies
 * instead of original objects. Working copy is produced from the current version using clone (shallow copy) method.
 * Clone will work correctly if components of the objects have primitive type (int, double,...) or are immutable (String). 
 * Using java.util.Date type is possible only if you do not try to update its components. 
 * Instead of normal Java references, you should use references to correspondent CVersionHistory (it allows to preserve 
 * reference consistency if new version of reference object is created).
 * CDatabase class also supports links (components with Link type) and arrays. 
 * Fields with <i>value type</i> (class implementing IValue interface) can be used only if them are 
 * immutable or cloneable (implement org.garret.perst.ICloneable interface). 
 * Using any other data type including Perst collection classes is not supported and can cause unpredictable behavior.
 */
public class CDatabase { 
    /** 
     * Open the database. This method initialize database if it not initialized yet.
     * @param storage opened storage. Storage should be either empty (non-initialized, either
     * previously initialized by the this method. It is not possible to open storage with 
     * root object other than RootObject  created by this method.
     * @param fullTextIndexPath path to the directory containing Lucene database. If null, then Lucene index will be stored 
     * inside the main Perst storage.
     */
    public void open(Storage storage, String fullTextIndexPath) 
    { 
        this.storage = storage;
        storage.setProperty("perst.concurrent.iterator", Boolean.TRUE);
        root = (RootObject)storage.getRoot();
        typeMap = new HashMap<Class,TableDescriptor>();
        if (root == null) { 
            root = new RootObject(storage);
            storage.setRoot(root);
        } else { 
            for (TableDescriptor desc : root.tables) { 
                typeMap.put(desc.type, desc);                
            }
            for (TableDescriptor desc : typeMap.values()) { 
                buildInheritanceHierarchy(desc);
            }
        }
        openFullTextIndex(root, fullTextIndexPath);
        storage.commit();
    }

    /**
     * Close the database. All uncommitted transaction will be aborted.
     * Close full text index and storage. 
     */
    public void close() 
    { 
        try { 
            if (indexWriter != null) { 
                indexWriter.close();
                indexWriter = null;
            }
            if (indexReader != null) { 
                indexReader.close();
                indexReader = null;
            }
            if (dir != null) { 
                dir.close();
                dir = null;
            }
        } catch (IOException x) { 
            throw new IOError(x);
        }
        storage.close();
        storage = null;

        root = null;
        typeMap = null;
        analyzer = null;
    }        

    /**
     * Start new transaction. All changes can be made only within transaction context. 
     * Transaction context is associated with thread. It means that one thread can run only one transaction and
     * one transaction can be handled only by one thread. 
     * Transaction is observing database state at the moment of the transaction start. Any changes done by
     * other transactions (committed and uncommitted) after this moment are not visible for the current transaction.
     * CDatabase uses optimistic access control. It means that transaction may be aborted if conflict 
     * is detected during transaction commit.
     */
    public void beginTransaction() 
    {
        beginTransaction(root.getLastTransactionId());
    }

    /**
     * Start transaction with the given identifier. 
     * Identifier of the transaction can be obtained using CVersion.getTransactionId() method.
     * Starting transaction with ID of previously committed transaction allows to obtain snapshot
     * of the database at the moment of this transaction execution. All search methods
     * using default version selector (referencing current version) and CVersionHistory.getCurrent()
     * method will select versions which were current at the moment of this transaction execution.
     * @param transId identifier of previously committed transaction
     * @exception TransactionAlreadyStartedException when thread attempts to start new transaction without committing or 
     * aborting previous one. Transaction should be explicitly finished using CDatabase.commit or CDatabase.rollback method.
     * Each thread should start its own transaction, it is not possible to share the same transaction by more than one threads.
     * It is possible to call beginTransaction several times without commit or rollback only if previous transaction was read-only 
     * - didn't change any object
     */
    public synchronized void beginTransaction(long transId) 
    {
        TransactionContext ctx = getTransactionContext();
        if (ctx == null) { 
            ctx = new TransactionContext(this);
            transactionContext.set(ctx);
        }
        ctx.beginTransaction(transId, ++maxTransSeqNo);
    }

    /**
     * Commit transaction. 
     * @exception ConflictExpetion is thrown if object modified by this transaction was also changed by some other 
     * previously committed transaction 
     * @exception NotUniqueException is thrown if indexable field with unique constraint of inserted or updated object 
     * contains the same value as current version of some other instance of this class (committed by another transact).
     * Please notice that CDatabase is not able to detect unique constraint violations within one transaction - if it
     * inserts two instances with same value of indexable field.
     * @return identifier assigned to this transaction or 0 if there is no active transaction. 
     * This identifier can be used to obtain snapshot of the database at the moment of this transaction execution.
     */
    public long commitTransaction() throws ConflictException, NotUniqueException 
    { 
        TransactionContext ctx = getTransactionContext();
        if (ctx == null || ctx.transId == TransactionContext.IMPLICIT_TRANSACTION_ID) { 
            return 0;
        }
        if (ctx.isEmptyTransaction() && !root.isModified()) { 
            ctx.endTransaction();
            return ctx.transId;
        }
        root.exclusiveLock();
        try { 
            Collection<CVersion> workSet = ctx.getWorkingCopies();
            long transId = ctx.transId;
            for (CVersion v : workSet) {
                if ((v.flags & CVersion.NEW) == 0 && v.history.getLast().transId > transId) { 
                    throw new ConflictException(v);
                }
                TableDescriptor desc = lookupTable(v.getClass());
                if (desc != null) { 
                    desc.checkConstraints(v);
                }
            }
            if (ctx.seqNo == minTransSeqNo) { 
                minTransSeqNo += 1;
                if (ctx.transId > lastActiveTransId) { 
                    lastActiveTransId = ctx.transId;
                }
            }
            transId = root.newTransactionId();
            for (CVersion v : workSet) { 
                CVersionHistory vh = v.history;
                TableDescriptor desc = getTable(v.getClass());
                int flags = v.flags;

                v.transId = transId;
                v.id = vh.getNumberOfVersions() + 1;
                v.flags &= ~(CVersion.WORKING_COPY|CVersion.NEW);
                v.date = new Date();
                vh.add(v);

                if ((flags & (CVersion.NEW|CVersion.DELETED)) == CVersion.NEW) { 
                    TableDescriptor td = desc;
                    do { 
                        td.classExtent.add(vh);
                    } while ((td = td.supertable) != null);                        
                } else if (vh.limited) { 
                    CVersion old;
                    while ((old = vh.get(CVersion.FIRST_VERSION_ID)).transId < lastActiveTransId) {
                        desc.excludeFromIndices(old);
                        excludeFromFullTextIndex(desc, old);
                        vh.remove(CVersion.FIRST_VERSION_ID);
                        old.deallocate();
                    }
                }
                if ((flags & CVersion.DELETED) == 0) {
                    desc.includeInIndices(v); 
                    includeInFullTextIndex(desc.buildDocument(v));
                }
            }      
            try {
                if (indexWriter != null) { 
                    indexWriter.close();
                    indexWriter = null;
                }
            } catch (IOException x) { 
                throw new IOError(x);
            }
            storage.commit();
            return transId;
        } finally { 
            root.unlock();         
            ctx.endTransaction();
        }
    }
    
    /**
     * Rollback transaction
     */
    public void rollbackTransaction() 
    { 
        TransactionContext ctx = getTransactionContext();
        if (ctx != null) { 
            root.exclusiveLock();
            try { 
                if (ctx.seqNo == minTransSeqNo) { 
                    minTransSeqNo += 1;
                    if (ctx.transId > lastActiveTransId) { 
                        lastActiveTransId = ctx.transId;
                    }
                }
            } finally { 
                root.unlock();         
                ctx.endTransaction();
            }
        }
    }

    /**
     * Insert new record in the database
     * @param record inserted record
     * @exception ObjectAlreadyInsertedException when object is part of some other version history
     * @exception TransactionNotStartedException if transaction was not started by this thread using CDatabase.beginTransaction
     */
    public <T extends CVersion> void insert(T record) 
    { 
        getWriteTransactionContext().insert(record);
    }

    /**
     * Update the record. 
     * @param record updated record
     * @return working copy of the specified version
     * @exception AmbiguousVersionException when some other version from the same version history was already updated by the current transaction
     * @exception TransactionNotStartedException if transaction was not started by this thread using CDatabase.beginTransaction
     */
    public <T extends CVersion> T update(T record) { 
        return (T)record.update();
    }

    /**
     * Mark current version as been deleted.
     * @param record working copy of the current version or current version itself. In the last case work copy is created
     * @exception NotCurrentVersionException is thrown if specified version is not current in the version history
     * @exception TransactionNotStartedException if transaction was not started by this thread using CDatabase.beginTransaction
     */
    public void delete(CVersion record) { 
        record.delete();
    }

    /**
     * Extract single result from the returned result set.
     * It should be applied as pipeline to CDatabase.select/CDatabase.find/CDatabase.getRecords/Query.execute methods:
     * <code>
     *    MyClass obj = db.getSingleton(db.find(MyClass.class, "key", value));
     * </code>
     * @param iterator result set iterator returned by CDatabase.select/CDatabase.find/CDatabase.getRecords/Query.execute
     * methods
     * @return selected object if result set contains exactly one object or null if result set is empty
     * @exception SingletonException if result set contains more than one element
     */     
    public <T extends CVersion> T getSingleton(Iterator<T> iterator) throws SingletonException
    { 
        root.sharedLock();
        try { 
            T obj = null;
            if (iterator.hasNext()) { 
                obj = iterator.next();
                if (iterator.hasNext()) {  
                    throw new SingletonException();
                }
            }
            return obj;
        } finally { 
            root.unlock();
        } 
    }

    /**
     * Enumeration specifying version sort order in the result set.
     * Sorting is performed by version identifier
     */
    public enum VersionSortOrder 
    {
        ASCENT(1),
        NONE(0),
        DESCENT(-1);

        int delta;

        VersionSortOrder(int delta) { 
            this.delta = delta;
        }        
    }    

    /**
     * Convert returned result set to the List collection. 
     * It should be applied as pipeline to CDatabase.select/CDatabase.find/CDatabase.getRecords/Query.execute methods:
     * <code>
     *    List<MyClass> list = db.toList(db.select(MyClass.class, "price between 100 and 1000 and amount > 100"));
     * </code>
     * Result set iterator usually selects records in Perst in lazy mode, so execution of query is very 
     * fast but fetching each element requires some additional calculations. 
     * Using this method you can force loading of all result set elements. So you will know
     * precise number of selected records, can access them in any order and without extra overhead.
     * The payment for these advantages is increased time of query execution and amount of memory needed
     * to hold all selected objects.
     * @param iterator result set iterator returned by CDatabase.select/CDatabase.find/CDatabase.getRecords/Query.execute
     * methods
     * @return List containing all selected objects
     */     
    public <T extends CVersion> List<T> toList(Iterator<T> iterator) 
    { 
        return toList(iterator, Integer.MAX_VALUE);
    }

    /**
     * Converted returned result set to List collection with limit for number of fetched records. 
     * It should be applied as pipeline to CDatabase.select/CDatabase.find/CDatabase.getRecords/Query.execute methods:
     * <code>
     *    List<MyClass> list = db.toList(db.select(MyClass.class, "price between 100 and 1000 and amount > 100"));
     * </code>
     * Result set iterator usually selects records in Perst in lazy mode, so execution of query is very 
     * fast but fetching each element requires some additional calculations. 
     * Using this method you can force loading of all result set elements. So you will know
     * precise number of selected results, can access them in any order and without extra overhead.
     * The payment for these advantages is increased time of query execution and amount of memory needed
     * to hold all selected objects.
     * @param iterator result set iterator returned by CDatabase.select/CDatabase.find/CDatabase.getRecords/Query.execute
     * methods
     * @param limit result list limit
     * @return List containing selected objects, if number of selected objects is larger than specified <code>limit</code>, 
     * then only first <code>limit</code> of them will be placed in the list and other will be ignored.
     */         
    public <T extends CVersion> List<T> toList(Iterator<T> iterator, int limit)
    { 
        ArrayList<T> list = new ArrayList();
        root.sharedLock();
        try { 
            while (--limit >= 0 && iterator.hasNext()) { 
                list.add(iterator.next());
            } 
            return list;
        } finally { 
            root.unlock();
        } 
    }

    /**
     * Converted returned result set to array.
     * It should be applied as pipeline to CDatabase.select/CDatabase.find/CDatabase.getRecords/Query.execute methods:
     * <code>
     *    MyClass[] arr = db.toArray(new MyClass[0], db.select(MyClass.class, "price between 100 and 1000 and amount > 100", 
     *                                                         VersionSortOrder.ASCENT));
     * </code>
     * Result set iterator usually selects records in Perst in lazy mode, so execution of query is very 
     * fast but fetching each element requires some additional calculations. 
     * Using this method you can force loading of all result set elements. So you will know
     * precise number of selected results, can access them in any order and without extra overhead.
     * The payment for these advantages is increased time of query execution and amount of memory needed
     * to hold all selected objects.
     * Specifying sort order allows for example to select the first/last version satisfying search criteria.
     * @param arr the array into which the elements of the list are to be stored, 
     * if it is big enough; otherwise, a new array of the same runtime type is allocated for this purpose.
     * @param iterator result set iterator returned by CDatabase.select/CDatabase.find/CDatabase.getRecords/Query.execute
     * methods
     * @param order version sort order: ASCENT, DESCENT or NONE. Array elements will be sorted by version identifiers. 
     * If sort order is not specified NONE (then records in the result array will be in the same order as returned by the iterator)
     * @return an array containing selected objects in the given order
     */         
    public <T extends CVersion> T[] toArray(T[] arr, Iterator<T> iterator, VersionSortOrder order) 
    { 
        ArrayList<T> list = new ArrayList();
        root.sharedLock();
        try { 
            while (iterator.hasNext()) { 
                list.add(iterator.next());
            }
        } finally { 
            root.unlock();
        }
        arr = list.toArray(arr);
        if (order != VersionSortOrder.NONE) { 
            final int delta = order.delta;
            Arrays.sort(arr, new Comparator<T>() { 
                            public int compare(T v1, T v2) { 
                                return v1.transId < v2.transId ? -delta : v1.transId == v2.transId ? 0 : delta;
                            }
                        });
        }
        return arr;
    }
    
    /**
     * Select current version of records from specified table matching specified criteria
     * @param table class corresponding to the table
     * @param predicate search predicate
     * @return iterator through selected records. This iterator doesn't support remove() method.
     * If there are no instances of such class in the database, then empty iterator is returned
     * @exception CompileError exception is thrown if predicate is not valid JSQL exception
     * @exception JSQLRuntimeException exception is thrown if there is runtime error during query execution
     */
    public <T extends CVersion> IterableIterator<T> select(Class table, String predicate) 
    { 
        return select(table, predicate, VersionSelector.CURRENT);
    }
        
    /**
     * Select records from the specified table using version selector
     * @param table class corresponding to the table
     * @param predicate search predicate
     * @param selector version selector
     * @return iterator through selected records. This iterator doesn't support remove() method.
     * If there are no instances of such class in the database, then empty iterator is returned
     * @exception CompileError exception is thrown if predicate is not valid JSQL exception
     * @exception JSQLRuntimeException exception is thrown if there is runtime error during query execution
     */
    public <T extends CVersion> IterableIterator<T> select(Class table, String predicate, VersionSelector selector)
    { 
        Query q = prepare(table, predicate);
        return q.execute(getRecords(table, selector));
    }

    /**
     * Prepare JSQL query. Prepare is needed for queries with parameters. Also
     * preparing query can improve speed if query will be executed multiple times
     * (using prepare, it is compiled only once).
     * To execute prepared query, you should use Query.execute(db.getRecords(TABLE.class)) method
     * @param table class corresponding to the table
     * @param predicate search predicate
     * @return prepared query
     * @exception CompileError exception is thrown if predicate is not valid JSQL exception
     */
    public <T extends CVersion> Query<T> prepare(Class table, String predicate) 
    { 
        return prepare(table, predicate, VersionSelector.CURRENT);
    }

    /**
     * Prepare JSQL query. Prepare is needed for queries with parameters. Also
     * preparing query can improve speed if query will be executed multiple times
     * (using prepare, it is compiled only once).
     * To execute prepared query, you should use Query.execute(db.getRecords(TABLE.class)) method
     * @param table class corresponding to the table
     * @param predicate search predicate
     * @param selector version selector
     * @return prepared query
     * @exception CompileError exception is thrown if predicate is not valid JSQL exception
     */
    public <T extends CVersion> Query<T> prepare(Class table, String predicate, VersionSelector selector) 
    { 
        Query q = storage.createQuery();
        q.prepare(table, predicate);            
        TableDescriptor desc = lookupTable(table);
        if (desc != null) { 
            desc.registerIndices(q, root, selector);
        }
        return q;
    }

    /** 
     * Get iterator through current version of all table records
     * @param table class corresponding to the table
     * @return iterator through all table records. If there are no instances of such class in the database, then empty
     * iterator is returned
     */
    public <T extends CVersion> IterableIterator<T> getRecords(Class table)
    {
        return getRecords(table, VersionSelector.CURRENT);
    }

    /** 
     * Get iterator through all records of the table using specified version selector
     * @param table class corresponding to the table
     * @param selector version selector
     * @return iterator through all table records. If there are no instances of such class in the database, then empty
     * iterator is returned
     */
    public <T extends CVersion> IterableIterator<T> getRecords(Class table, VersionSelector selector)
    {         
        root.sharedLock();
        try { 
            TableDescriptor desc = lookupTable(table);        
            IterableIterator<T> iterator = (desc == null) 
                ? new EmptyIterator<T>() 
                : new ExtentIterator<T>(desc.iterator(), root, selector);
            return iterator;
        } finally { 
            root.unlock();
        } 
    }

    /**
     * Select current version from specified table by key
     * @param table class corresponding to the table
     * @param field indexed field
     * @param key key value
     * @return iterator through selected records. This iterator doesn't support remove() method.
     * If there are no instances of such class in the database, then empty iterator is returned
     * @exception NoSuchIndexException if there is no index for the specified field
     */
    public <T extends CVersion> IterableIterator<T> find(Class table, String field, Key key) throws NoSuchIndexException 
    {
        return find(table, field, key, VersionSelector.CURRENT);
    }

    /**
     * Select records from the specified table by key using version selector
     * @param table class corresponding to the table
     * @param field indexed field
     * @param key key value
     * @param selector version selector
     * @return iterator through selected records. This iterator doesn't support remove() method.
     * If there are no instances of such class in the database, then empty iterator is returned
     * @exception NoSuchIndexException if there is no index for the specified field
     */
    public <T extends CVersion> IterableIterator<T> find(Class table, String field, Key key, VersionSelector selector) throws NoSuchIndexException 
    {
        TableDescriptor desc = lookupTable(table);
        if (desc == null) { 
            return new EmptyIterator<T>();
        }
        TableDescriptor.IndexDescriptor idesc = desc.findIndex(field);
        if (idesc == null) { 
            throw new NoSuchIndexException(field);
        }
        key = idesc.checkKey(key);
        return new IndexFilter<T>(idesc, root, selector).iterator(key, key, GenericIndex.ASCENT_ORDER);
    }

    /**
     * Select records from the specified table by key range using version selector
     * @param table class corresponding to the table
     * @param field indexed field
     * @param min minimal key value
     * @param max maximal key value
     * @param selector version selector
     * @param order key sort order: GenericIndex.ASCENT_ORDER or GenericIndex.DESCENT_ORDER
     * @return iterator through selected records. This iterator doesn't support remove() method.
     * If there are no instances of such class in the database, then empty iterator is returned
     * @exception NoSuchIndexException if there is no index for the specified field
     */
    public <T extends CVersion> IterableIterator<T> find(Class table, String field, Key min, Key max, VersionSelector selector, int order)
    {
        TableDescriptor desc = lookupTable(table);
        if (desc == null) { 
            return new EmptyIterator<T>();
        }
        TableDescriptor.IndexDescriptor idesc = desc.findIndex(field);
        if (idesc == null) { 
            throw new NoSuchIndexException(field);
        }
        return new IndexFilter<T>(idesc, root, selector).iterator(idesc.checkKey(min), idesc.checkKey(max), order);
    }

    /**
     * Perform full text search through the current version of all objects in the database
     * @param query full text search search query. It should be compliant with Lucene query syntax and may
     * refer to the particular fields or to any full text searchable field:
     * <ul>
     * <li>"name:John" - select document with field "name" containing word "John"</li>
     * <li>"title:magic AND author:Clark" - select document with field "title" containing word "magic" and field "author"
     *                                     containing word "Clark"</li>
     * <li>"atomic nuclear power" - select any document which full text searchable fields contain any of the specified words</li>
     * <li>"Class:com.eshop.Player AND description:DivX" - select objects with class com.eshop.Player which description contains word "DivX"</li>
     * </ul>
     * @param limit search result limit 
     * @return array with matched documents
     * @exception FullTextSearchQuerySyntaxError if query syntax is invalid
     */
    public FullTextSearchResult[] fullTextSearch(String query, int limit)
    {
        return fullTextSearch(query, limit, VersionSelector.CURRENT, VersionSortOrder.NONE);
    }

    /**
     * Perform full text search using version selector
     * @param query full text search search query. It should be compliant with Lucene query syntax and may
     * refer to the particular fields or to any full text searchable field:
     * <ul>
     * <li>"name:John" - select document with field "name" containing word "John"</li>
     * <li>"title:magic AND author:Clark" - select document with field "title" containing word "magic" and field "author"
     *                                     containing word "Clark"</li>
     * <li>"atomic nuclear power" - select any document which full text searchable fields contain any of the specified words</li>
     * <li>"Class:com.eshop.Player AND description:DivX" - select objects with class com.eshop.Player which description contains word "DivX"</li>
     * </ul>
     * @param limit search result limit 
     * @param selector version selector
     * @param order result set versions sort order (if VersionSortOrder.NONE then order of documents returned by Lucene 
     * is preserved - documents are sorted by score)
     * @return array with matched documents
     * @exception FullTextSearchQuerySyntaxError if query syntax is invalid
     */
    public FullTextSearchResult[] fullTextSearch(String query, int limit, VersionSelector selector, VersionSortOrder order)
    {
        ArrayList<FullTextSearchResult> result = null;
        root.sharedLock();
        try { 
            Hits hits;
            try { 
                IndexSearcher searcher = new IndexSearcher(getIndexReader());
                if (selector.kind == VersionSelector.Kind.TimeSlice) { 
                    long from = selector.from != null ? selector.from.getTime() : 0;
                    long till = selector.till != null ? selector.till.getTime() : System.currentTimeMillis();
                    StringBuilder sb = new StringBuilder("Created:[");
                    sb.append(DateTools.timeToString(from, DateTools.Resolution.MINUTE));
                    sb.append(" TO ");
                    sb.append(DateTools.timeToString(till, DateTools.Resolution.MINUTE));
                    sb.append("] AND (");
                    sb.append(query);
                    sb.append(')');
                    query = sb.toString();
                }
                //QueryParser parser = new MultiFieldQueryParser(new String[]{"Any"}, analyzer); 
                QueryParser parser = new QueryParser("Any", analyzer);
                hits = searcher.search(parser.parse(query));
            } catch (IOException x) {
                throw new IOError(x);
            } catch (ParseException x) { 
                throw new FullTextSearchQuerySyntaxError(x);
            } 
            result = new ArrayList<FullTextSearchResult>(limit > 0 ? limit : (limit = hits.length()));
            Iterator<FullTextSearchResult> iterator = new FullTextSearchIterator(hits, storage, selector);
            while (--limit >= 0 && iterator.hasNext()) { 
                result.add(iterator.next());
            }
        } finally { 
            root.unlock();
        } 
        FullTextSearchResult[] arr = result.toArray(new FullTextSearchResult[result.size()]);
        if (order != VersionSortOrder.NONE) { 
            final int delta = order.delta;
            Arrays.sort(arr, new Comparator<FullTextSearchResult>() { 
                            public int compare(FullTextSearchResult r1, FullTextSearchResult r2) { 
                                long t1 = r1.getVersion().transId;
                                long t2 = r2.getVersion().transId;
                                return t1 < t2 ? -delta : t1 == t2 ? 0 : delta;
                            }
                        });
        }
        return arr;
    }

    /**
     * Restore full text search index.
     * If the full text search index is located in file system directory it may be get out of sync with the Perst database or even be 
     * corrupted in case of failure. In this case index can be reconstructed using this method.
     */
    public synchronized void restoreFullTextIndex()
    {
        root.exclusiveLock();
        try { 
            try { 
                if (indexWriter != null) { 
                    indexWriter.close();
                }
                if (indexReader != null) { 
                    indexReader.close();
                    indexReader = null;
                }
                indexWriter = new IndexWriter(dir, analyzer, true);
                for (TableDescriptor table : root.tables) {
                    for (CVersionHistory<?> vh : table) {
                        for (CVersion v : vh) { 
                            if (!v.isDeletedVersion()) { 
                                Document doc = table.buildDocument(v);
                                if (doc != null) { 
                                    indexWriter.addDocument(doc);
                                }
                            }
                        }
                    }
                }                                       
                indexWriter.optimize();
            } catch (IOException x) {
                throw new IOError(x);
            }
        } finally { 
            root.unlock();
        }
    }

    /**
     * Optimize full text index. This method can be periodically called by application (preferably in idle time)
     * to optimize full text search index
     */
    public synchronized void optimizeFullTextIndex() 
    {
        root.exclusiveLock();
        try { 
            try { 
                getIndexWriter().optimize();
            } catch (IOException x) {
                throw new IOError(x);
            }
        } finally { 
            root.unlock();
        }
    }
        

    /**
     * Get storage associated with this database
     * @return underlying storage
     */
    public Storage getStorage() 
    { 
        return storage;
    }


    /**
     * Get resource used to synchronize access to the database
     */
    public IResource getResource() 
    { 
        return root;
    }

    /**
     * Get user data. Continuous uses Perst root objects for its own purposes. 
     * But this methods allows application to specify its own root and store in the Perst 
     * storage non-versioned objects. 
     * @return reference to the persistent object previously stored by setUserData()
     */
    public IPersistent getUserData() 
    { 
        root.sharedLock();
        try { 
            return root.userData;
        } finally { 
            root.unlock();
        }
    }

     /**
     * Set user data. Continuous uses Perst root objects for its own purposes. 
     * But this methods allows application to specify its own root and store in the Perst 
     * storage non-versioned objects. 
     * @param obj reference to the persistent object which will be stored in Perst root object
     */
    public void setUserData(IPersistent obj) 
    { 
        root.exclusiveLock();
        try { 
            root.userData = obj;
            root.modify();
        } finally { 
            root.unlock();
        }
    }
    
    /**
     * Get collection all of field names 
     */
    public Collection<String> getAllFullTextSearchanbleFieldNames() throws Exception {
       return getIndexReader().getFieldNames(IndexReader.FieldOption.INDEXED);
    }

    /**
     * Static instance of the database which can be used by application working with the single database
     */
    public static CDatabase instance = new CDatabase();


    static TransactionContext getTransactionContext() 
    { 
        return transactionContext.get();
    }

    static TransactionContext getWriteTransactionContext() 
    { 
        TransactionContext ctx = transactionContext.get();
        if (ctx == null) { 
            throw new TransactionNotStartedException();
        }            
        return ctx;
    }

    void openFullTextIndex(RootObject root, String path)
    {
        boolean create;
        if (path == null) { 
            // Store Lucene index in Perst database
            create = root.catalogue == null;
            dir = new PerstDirectory(root);
        } else { 
            File file = new File(path);
            create = !file.exists();
            try { 
                dir = FSDirectory.getDirectory(path);
            } catch (IOException x) {
                throw new IOError(x);
            }
        }
        dir.setLockFactory(NoLockFactory.getNoLockFactory());
        analyzer = new StandardAnalyzer();
        try { 
            indexWriter = new IndexWriter(dir, analyzer, create);
        } catch (IOException x) {
            throw new IOError(x);
        }
    }

    void excludeFromFullTextIndex(TableDescriptor desc, CVersion v) 
    {
        if (desc.isFullTextSearchable()) { 
            Term term = new Term("Oid", Integer.toString(v.getOid()));
            try { 
                getIndexReader().deleteDocuments(term);
            } catch (IOException x) { 
                throw new IOError(x);
            }
        }
    }
    

    void includeInFullTextIndex(Document doc)
    {
        if (doc != null) { 
            try { 
                getIndexWriter().addDocument(doc);
            } catch (IOException x) {
                throw new IOError(x);
            }
        }
    }

    synchronized TableDescriptor lookupTable(Class type)
    {
        return typeMap.get(type);
    }

    synchronized TableDescriptor getTable(Class type)
    {
        TableDescriptor desc = typeMap.get(type);
        if (desc == null) { 
            desc = new TableDescriptor(storage, type);
            buildInheritanceHierarchy(desc);
            typeMap.put(type, desc);
            root.tables.add(desc);
        }
        return desc;
    }

    void buildInheritanceHierarchy(TableDescriptor desc) { 
        Class superclass = desc.type.getSuperclass();
        if (superclass != CVersion.class) {             
            desc.supertable = getTable(superclass);
        }
    }
           
    synchronized IndexWriter getIndexWriter() throws IOException 
    {
        if (indexWriter == null) { 
            if (indexReader != null) { 
                indexReader.close();
                indexReader = null;
            }
            indexWriter = new IndexWriter(dir, analyzer, false);
        }
        return indexWriter;
    }

    synchronized IndexReader getIndexReader() throws IOException 
    {             
        if (indexReader == null) { 
            if (indexWriter != null) { 
                indexWriter.close();
                indexWriter = null;
            }
            indexReader = IndexReader.open(dir);
        }
        return indexReader;
    }

    static ThreadLocal<TransactionContext> transactionContext = new ThreadLocal<TransactionContext>();

    Storage storage;
    RootObject root;
    HashMap<Class,TableDescriptor> typeMap;   
    IndexWriter indexWriter;
    IndexReader indexReader;
    StandardAnalyzer analyzer;
    Directory dir;
    long minTransSeqNo;
    long maxTransSeqNo;
    long lastActiveTransId;
}