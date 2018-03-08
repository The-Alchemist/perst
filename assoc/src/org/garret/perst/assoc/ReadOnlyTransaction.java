package org.garret.perst.assoc;

import org.garret.perst.*;
import org.garret.perst.fulltext.*;
import java.util.*;

/**
 * Read-only transaction.
 * AssocDB provides MURSIW (multiple readers single writer) isolation model.
 * It means that only one transaction can update the database at each moment of time, but multiple transactions
 * can concurrently read it.
 *
 * All access to the database (read or write) should be performed within transaction body.
 * For write access it is enforced by placing all update methods in ReadWriteTransaction class.
 * But for convenience reasons read-only access methods are left in Item class. It is responsibility of programmer to check that 
 * them are not invoked outside transaction body.
 * 
 * Transaction should be explicitly started by correspondent method of AssocDB and then it has to be either committed, 
 * either aborted. In any case, it can not be used any more after commit or rollback - you should start another transaction.
 */
public class ReadOnlyTransaction
{
    /** 
     * Locate items matching specified search criteria
     * @param predicate search condition
     * @return iterator through selected results (it also implemented Iterable interface to allow
     * to use it in <code>for (T x:collection)</code> statement. But you should not use this iterable more than once.
     * Also IterableIterator class provides some convenient methods for fetching first result, counting results and 
     * extraction them to array
     */
    public IterableIterator<Item> find(Predicate predicate) { 
        checkIfActive();
        return evaluate(predicate, false);
    }
    
    /** 
     * Locate items matching specified search criteria and sort them in desired order
     * @param predicate search condition
     * @param order sort criteria
     * @return array in specified sort order
     */
    public Item[] find(Predicate predicate, OrderBy ... order) 
    {
        ArrayList<Item> list = find(predicate).toList();
        Item[] items = list.<Item>toArray(new Item[list.size()]);
        Arrays.sort(items, new ItemComparator(order));
        return items;
    }

    /**
     * Get all items containing specified attribute in ascending order
     * @param name attribute name
     * @return iterator through all items containing specified attribute or null if there is not such attribute in the database
     */
    public IterableIterator<Item> getOccurrences(String name)
    {
        return getOccurrences(name, OrderBy.Order.Ascending);
    }

    /**
     * Get all items containing specified attribute in desired order
     * @param name attribute name
     * @param order ascending or descending sort order
     * @return iterator through all items containing specified attribute or null if there is not such attribute in the database
     */
    public IterableIterator<Item> getOccurrences(String name, OrderBy.Order order)
    { 
        checkIfActive();
        Integer idWrapper = db.name2id.get(name);
        if (idWrapper == null) { 
            return null;
        }
        Index<Item> index = (Index<Item>)db.storage.getObjectByOID(idWrapper.intValue());
        return index.iterator(null, null, order == OrderBy.Order.Ascending ? Index.ASCENT_ORDER : Index.DESCENT_ORDER);
    }

    /**
     * Parse and execute full text search query
     * @param query text of the query
     * @param maxResults maximal amount of selected documents
     * @param timeLimit limit for query execution time
     * @return result of query execution ordered by rank or null in case of empty or incorrect query
     */
    public FullTextSearchResult fullTextSearch(String query, int maxResults, int timeLimit)
    {
        checkIfActive();
        return db.root.fullTextIndex.search(query, db.language, maxResults, timeLimit);
    }

    /**
     * Locate all documents containing words started with specified prefix
     * @param prefix word prefix
     * @param maxResults maximal amount of selected documents
     * @param timeLimit limit for query execution time
     * @param sort whether it is necessary to sort result by rank
     * @return result of query execution ordered by rank (if sort==true) or null in case of empty or incorrect query     
     */
    public FullTextSearchResult fullTextSearchPrefix(String prefix, int maxResults, int timeLimit, boolean sort)
    {
        checkIfActive();
        return db.root.fullTextIndex.searchPrefix(prefix, maxResults, timeLimit, sort);
    }
    
    /**
     * Get iterator through full text index keywords started with specified prefix
     * @param prefix keyword prefix (use empty string to get list of all keywords)
     * @return iterator through list of all keywords with specified prefix
     */
    public Iterator<FullTextIndex.Keyword> getFullTextSearchKeywords(String prefix)
    {
        checkIfActive();
        return db.root.fullTextIndex.getKeywords(prefix);
    }

    /**
     * Get set of all verbs (attribute names) in the database
     * @return unordered set of of verbs in the database
     */
    public Set<String> getVerbs()
    {    
        checkIfActive();
        return db.name2id.keySet();
    }

    /**
     * Get attribute type. AssocDB requires that values of attribute in all objects have the same type.
     * You can not store for example "age" in  one item as number 35 and in other item - as string "5 months".
     * If it is really needed you have to introduce new attribute.
     * @param name attribute name
     * @return class of the attribute value (String, double or Item) or null if attribute with such name is not found in the database
     */
    public Class getAttributeType(String name)
    {
        checkIfActive();
        Integer idWrapper = db.name2id.get(name);
        if (idWrapper == null) { 
            return null;
        }
        Index<Item> index = (Index<Item>)db.storage.getObjectByOID(idWrapper.intValue());
        Class cls = index.getKeyType();
        if (cls == Object.class) { 
            cls = Item.class;
        }
        return cls;
    }
        
    /**
     * Commit this transaction.
     * It is not possible to use this transaction object after it is committed
     */
    public void commit() {
        checkIfActive();
        db.unlock();        
        active = false;
    }

    /**
     * Rollback this transaction (for read-only transaction is has the same effect as commit).
     * It is not possible to use this transaction object after it is rollbacked
     */
    public void rollback() { 
        checkIfActive();
        db.unlock();
        active = false;
    }

    protected final void checkIfActive() { 
        if (!active) { 
            throw new  IllegalStateException("Transaction is not active");
        }
    }

    class ItemComparator implements Comparator<Item>
    {
        ItemComparator(OrderBy[] orderBy) { 
            ids = new int[orderBy.length];
            for (int i = 0; i < orderBy.length; i++) { 
                Integer idWrapper = db.name2id.get(orderBy[i].name);
                if (idWrapper == null) { 
                    throw new NoSuchElementException();
                }
                ids[i] = idWrapper.intValue();
            }
            this.orderBy = orderBy;
        }
        
        public int compare(Item item1, Item item2) 
        { 
            for (int i = 0; i < ids.length; i++) {
                int id = ids[i];
                Comparable o1 = (Comparable)item1.getAttribute(id);
                Comparable o2 = (Comparable)item2.getAttribute(id);
                int diff = o1.compareTo(o2);
                if (diff != 0) { 
                    return orderBy[i].order == OrderBy.Order.Ascending ? diff : -diff;
                }
            }
            return 0;
        }

        OrderBy[] orderBy;    
        int[] ids;
    }

    protected Key createKey(Object value, boolean inclusive) 
    { 
        return (value instanceof String) 
            ? new Key(((String)value).toLowerCase(), inclusive)
            : (value instanceof Number) 
                ? new Key(((Number)value).doubleValue(), inclusive)
                : new Key((Item)value, inclusive);
    }

    protected IterableIterator<Item> getEmptyResultSet() 
    { 
        return new ArrayListIterator(new ArrayList<Item>(0));
    }

    protected IterableIterator<Item> evaluate(Predicate predicate, boolean sortNeeded) 
    {
        IterableIterator<Item> iterator = null;
        if (predicate instanceof Predicate.Compare) { 
            Predicate.Compare cmp = (Predicate.Compare)predicate;
            Integer id = db.name2id.get(cmp.name);
            if (id == null) { 
                return getEmptyResultSet();
            }
            Index<Item> index = (Index<Item>)db.storage.getObjectByOID(id.intValue());
            Object value = cmp.value;
            switch (cmp.oper) { 
            case Equals:
            {
                Key key = createKey(value, true);
                return index.iterator(key, key, Index.ASCENT_ORDER);
            }
            case LessThan:
                return index.iterator(null, createKey(value, false), Index.ASCENT_ORDER);
            case LessOrEquals:
                iterator = index.iterator(null, createKey(value, true), Index.ASCENT_ORDER);
                break;
            case GreaterThan:
                iterator = index.iterator(createKey(value, false), null, Index.ASCENT_ORDER);
                break;
            case GreaterOrEquals:
                iterator = index.iterator(createKey(value, true), null, Index.ASCENT_ORDER);
                break;
            case StartsWith:
                iterator = index.prefixIterator(((String)value).toLowerCase());
                break;
            case IsPrefixOf:
                iterator = new ArrayListIterator(index.prefixSearchList(((String)value).toLowerCase()));
                break;
            case InArray:
            {
                OidIterator dst = new OidIterator();
                int oid;
                if (value instanceof String[]) { 
                    String[] arr = (String[])value;
                    for (int i = 0; i < arr.length; i++) { 
                        Key key = new Key(arr[i].toLowerCase());
                        PersistentIterator src = (PersistentIterator)index.iterator(key, key, Index.ASCENT_ORDER);
                        while ((oid = src.nextOid()) != 0) { 
                            dst.add(oid);
                        }
                    }
                } else if (value instanceof double[]) { 
                    double[] arr = (double[])value;
                    for (int i = 0; i < arr.length; i++) { 
                        Key key = new Key(arr[i]);
                        PersistentIterator src = (PersistentIterator)index.iterator(key, key, Index.ASCENT_ORDER);
                        while ((oid = src.nextOid()) != 0) { 
                            dst.add(oid);
                        }
                    }
                } else { 
                    Item[] arr = (Item[])value;
                    for (int i = 0; i < arr.length; i++) { 
                        Key key = new Key(arr[i]);
                        PersistentIterator src = (PersistentIterator)index.iterator(key, key, Index.ASCENT_ORDER);
                        while ((oid = src.nextOid()) != 0) { 
                            dst.add(oid);
                        }
                    }
                }
                dst.uniq();
                return dst;
            }
            }
        } else if (predicate instanceof Predicate.And) { 
            Predicate.And and = (Predicate.And)predicate;
            return new JoinIterator(evaluate(and.left, true), evaluate(and.right, true));
        } else if (predicate instanceof Predicate.Or) { 
            Predicate.Or or = (Predicate.Or)predicate;
            return new MergeIterator(evaluate(or.left, true), evaluate(or.right, true));
        } else if (predicate instanceof Predicate.Between) { 
            Predicate.Between between = (Predicate.Between)predicate;
            Integer id = db.name2id.get(between.name);
            if (id == null) {
                return getEmptyResultSet();
            }
            Index<Item> index = (Index<Item>)db.storage.getObjectByOID(id.intValue());
            iterator = index.iterator(createKey(between.from, true), createKey(between.till, true), Index.ASCENT_ORDER);
        } else if (predicate instanceof Predicate.Match) { 
            Predicate.Match match = (Predicate.Match)predicate;
            iterator = new FullTextSearchResultIterator(db.root.fullTextIndex.search(match.query, db.language, match.maxResults, match.timeLimit));
        } else if (predicate instanceof Predicate.In) { 
            Predicate.In in = (Predicate.In)predicate;
            Integer id = db.name2id.get(in.name);
            if (id == null) { 
                return getEmptyResultSet();
            }
            Index<Item> index = (Index<Item>)db.storage.getObjectByOID(id.intValue());
            OidIterator dst = new OidIterator();
            for (Item item : evaluate(in.subquery, false)) {
                Key key = new Key(item);
                PersistentIterator src = (PersistentIterator)index.iterator(key, key, Index.ASCENT_ORDER);
                int oid;
                while ((oid = src.nextOid()) != 0) { 
                    dst.add(oid);
                }
            }
            dst.uniq();            
            return dst;
        } else { 
            return null;
        }
        return sortNeeded ? sortResult(iterator) : iterator;
    }
    
    protected IterableIterator<Item> sortResult(IterableIterator<Item> iterable) 
    { 
        PersistentIterator src = (PersistentIterator)iterable;
        OidIterator dst = new OidIterator();
        int oid;
        while ((oid = src.nextOid()) != 0) { 
            dst.add(oid);
        }
        dst.sort();
        return dst;
    }

    class OidIterator extends IterableIterator<Item> implements PersistentIterator
    {
        OidIterator() 
        {
            oids = new int[1024];
        }

        final void add(int oid) 
        { 
            if (n == oids.length) { 
                int[] newOids = new int[n*2];
                System.arraycopy(oids, 0, newOids, 0, n);
                oids = newOids;
            }
            oids[n++] = oid;
        }

        final void sort() 
        { 
            Arrays.sort(oids, 0, n);
        }

        final void uniq() 
        { 
            if (n > 1) { 
                sort();
                int i = 0;
                int[] buf = oids;
                for (int j = 1, k = n; j < k; j++) { 
                    if (buf[j] != buf[i]) { 
                        buf[++i] = buf[j];
                    }
                }
                n = i + 1;
            }
        }

        public int nextOid() { 
            return i < n ? oids[i++] : 0;
        }

        public boolean hasNext() { 
            return i < n;
        }
        
        public Item next() { 
            if (i >= n) { 
                throw new NoSuchElementException();
            }
            return (Item)db.storage.getObjectByOID(oids[i++]);
        }

        int[] oids;
        int   i, n;
    }
    
    class ArrayListIterator extends IterableIterator<Item> implements PersistentIterator
    {
        ArrayListIterator(ArrayList<Item> list) { 
            iterator = list.iterator();
        }
        
        public boolean hasNext() { 
            return iterator.hasNext();
        }

        public Item next() {
            return iterator.next(); 
        }

        public int nextOid() { 
            return hasNext() ? next().getOid() : 0;
        }

        Iterator<Item> iterator;
    }

    class FullTextSearchResultIterator extends IterableIterator<Item> implements PersistentIterator
    {
        FullTextSearchResultIterator(FullTextSearchResult result) { 
            this.result = result;
        }
        
        public boolean hasNext() { 
            return i < result.hits.length;
        }

        public Item next() {
            if (!hasNext()) { 
                throw new NoSuchElementException();
            }
            return (Item)result.hits[i++].getDocument(); 
        }

        public int nextOid() { 
            return hasNext() ? result.hits[i++].oid : 0;
        }

        FullTextSearchResult result;
        int i;
    }

    class JoinIterator extends IterableIterator<Item> implements PersistentIterator
    {
        JoinIterator(IterableIterator<Item> i1, IterableIterator<Item> i2) { 
            this.i1 = (PersistentIterator)i1;
            this.i2 = (PersistentIterator)i2;
            oid = join();
        }

        private final int join() {
            int oid1, oid2 = 0;
            while ((oid1 = i1.nextOid()) != 0) { 
                while (oid2 < oid1) { 
                    if ((oid2 = i2.nextOid()) == 0) {
                        return 0;
                    }
                }
                if (oid2 == oid1) { 
                    return oid1;
                }
            }
            return 0;
        }

        public int nextOid() { 
            int curr = oid;
            if (curr != 0) { 
                oid = join();
            }
            return curr;
        }
         
        public boolean hasNext() { 
            return oid != 0;
        }
         
        public Item next() { 
            if (oid == 0) { 
                throw new NoSuchElementException();
            }
            Item item = (Item)db.storage.getObjectByOID(oid);
            oid = join();
            return item;
        }

        int oid;
        PersistentIterator i1, i2;
    }

    class MergeIterator extends IterableIterator<Item> implements PersistentIterator
    {
        MergeIterator(IterableIterator<Item> left, IterableIterator<Item> right) { 
            i1 = (PersistentIterator)left;
            i2 = (PersistentIterator)right;
            oid1 = i1.nextOid();
            oid2 = i2.nextOid();
        }

        public int nextOid() { 
            int oid = 0;
            if (oid1 < oid2) { 
                if (oid1 == 0) { 
                    oid = oid2;
                    oid2 = i2.nextOid();
                } else { 
                    oid = oid1;
                    oid1 = i1.nextOid();
                }
            } else if (oid2 != 0) { 
                oid = oid2;
                oid2 = i2.nextOid();
                if (oid1 == oid) { 
                    oid1 = i1.nextOid();
                }
            }
            return oid;
        }
         
        public boolean hasNext() { 
            return (oid1|oid2) != 0;
        }
         
        public Item next() { 
            int oid = nextOid();
            if (oid == 0) { 
                throw new NoSuchElementException();
            }
            return (Item)db.storage.getObjectByOID(oid);
        }

        int oid1, oid2;
        PersistentIterator i1, i2;
    }

    protected AssocDB db;
    protected boolean active;

    protected ReadOnlyTransaction(AssocDB db) 
    { 
        this.db = db;
        active = true;
    }
}
    