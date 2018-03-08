package org.garret.perst.assoc;

import org.garret.perst.*;
import org.garret.perst.fulltext.*;
import java.util.*;

/**
 * Main class of this package. Emulation of associative database on top of Perst. 
 * Strictly speaking AssocDB doesn't implement pure associative data model, although
 * it provides some notions of this model (items, links,...)
 * The main goal of this database is to efficient storage for object with statically known format and complex relations between
 * them. It can be used to store XML data, objects with user-defined fields, ...
 * AssocDB allows to fetch all attributes of an objects in one read operation without some kind of joins.
 * It automatically index all object attributes to provide fast search (simple query language is used).
 * All kind of relation between objects ore provided: one-to-one, one-to-many, many-to-many.
 * AssocDB supports small relations (tens of members) as well as very large relation (millions of members).
 * Small relations are embedded inside object to reduce storage overhead and their increase access time.
 * Large relations are implemented using B-Tree. AssocDB automatically choose proper representation.
 * Inverse links are automatically maintained, enforcing consistency of references.
 * AssocDB provides MURSIW (multiple readers single writer) isolation model.
 * It means that only one transaction can update the database at each moment of time, but multiple transactions
 * can concurrently read it.
 */
public class AssocDB 
{
    /**
     * Start read-only transactions. All access to the database (read or write) should be performed within transaction body.
     * For write access it is enforced by placing all update methods in ReadWriteTransaction class.
     * But for convenience reasons read-only access methods are left in Item class. It is responsibility of programmer to check that 
     * them are not invoked outside transaction body.
     * @return transaction object
     */
    public ReadOnlyTransaction startReadOnlyTransaction()
    {
        root.sharedLock();
        return new ReadOnlyTransaction(this);         
    }

    /**
     * Start read write transaction
     * @return transaction object
     */ 
    public ReadWriteTransaction startReadWriteTransaction()
    {
        root.exclusiveLock();
        return new ReadWriteTransaction(this);         
    }

    /**
     * Set threshold for embedded relations.
     * AssocDB supports small relations (tens of members) as well as very large relation (millions of members).
     * Small relation are embedded inside object to reduce storage overhead and their increase access time.
     * Large relations are implemented using B-Tree. AssocDB automatically choose proper representation.
     * Initially relations are stored inside object.
     * When number of links from an object exceeds this threshold value, AssocDB 
     * removes links from the object and store it in external B-Tree index.
     * @param newTreshold new threshold value
     * @return old threshold value 
     */
    public int setEmbeddedRelationThreshold(int newTreshold) { 
        int oldThreshold = embeddedRelationThreshold;
        embeddedRelationThreshold = newTreshold;
        return oldThreshold;
    }

    /**
     * AssocDB constructor. You should open and close storage yourself. You are free to set some storage properties, 
     * storage listener and use some other storage administrative methods  like backup.
     * But you should <b>not</b>: <ol>
     * <li>specify your own root object for the storage</li>
     * <li>use Storage commit or rollback methods directly</li>
     * <li>modify storage using Perst classes not belonging to this package</li>
     * </ol>
     * @param storage opened Perst storage
     */
    public AssocDB(Storage storage)
    {
        this.storage = storage;
        embeddedRelationThreshold = DEFAULT_EMBEDDED_RELATION_THRESHOLD;
        language = DEFAULT_DOCUMENT_LANGUAGE;
        root = (Root)storage.getRoot();
        name2id = new HashMap<String,Integer>();
        id2name = new HashMap<Integer,String>();
        if (root == null) { 
            root = createRoot();
            storage.setRoot(root);
            storage.commit();
        } else {
            root.db = this;
            for (Map.Entry<Object,Index<Item>> e : root.attributes.entryIterator()) { 
                Integer id = new Integer(e.getValue().getOid());
                String name = ((String)e.getKey()).intern();
                name2id.put(name, id);
                id2name.put(id, name);
            }
        }
    }

    /**
     * Set document's languages: used in full text search index
     * @param newLanguage new document's language
     * @return previously set language
     */
    public String setLanguage(String newLanguage) 
    { 
        String prevLanguage = language;
        language = newLanguage;
        return prevLanguage;
    }


    void unlock() 
    { 
        root.unlock();
    }

    /**
     * Storage root class - used internally by AssocDB.
     * You can provide your own root class derived from Root by overriding AssocDB.createRoot method
     */
    public static class Root extends PersistentResource
    { 
        protected Index<Item> relations;
        protected Index<Index<Item>> attributes;
        protected FullTextIndex fullTextIndex;

        protected transient AssocDB db;

        protected Root() {}

        protected Root(AssocDB db) { 
            super(db.storage);
            this.db = db;
            attributes = db.storage.<Index<Item>>createIndex(String.class, true);
            relations = db.storage.<Item>createIndex(Long.class, false);
            fullTextIndex = db.createFullTextIndex();
        }
    }

    /**
     * Create index for the particular attribute. Override this method to create some custom indices..
     * @param name attribute name
     * @param type attribute type
     * @return new index
     */
    protected Index<Item> createIndex(String name, Class type) 
    { 
        return storage.<Item>createThickIndex(type);
    }

    /**
     * Create root object. Override this method to create your own root derived from Root class.
     * @return created root
     */
    protected Root createRoot() 
    {        
        return new Root(this);
    }

    /**
     * Create full text index. Override this method to customize full text search.
     * @return created full text index
     */
    protected FullTextIndex createFullTextIndex() 
    {
        return storage.createFullTextIndex();
    }

    /**
     * Create item. Override this method to create instance of your own class derived from Item. In such way 
     * you can statically add some application specific fields to each class.
     * @return created full text index
     */
    protected Item createItem() 
    {
        return new Item(this);
    }

    /**
     * Default value for embedded relations threshold
     */
    public static int DEFAULT_EMBEDDED_RELATION_THRESHOLD = 100;

    /**
     * Default document's language (used for full text search)
     */
    public static String DEFAULT_DOCUMENT_LANGUAGE = "en";

    protected Storage storage;
    protected Root root;
    protected Map<String,Integer> name2id;
    protected Map<Integer,String> id2name;
    protected int embeddedRelationThreshold;
    protected String language;
}
