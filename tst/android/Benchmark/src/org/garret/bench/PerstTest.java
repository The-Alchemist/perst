package org.garret.bench;

import java.io.PrintStream;

import org.garret.perst.*;

/*
 * Record used in this example contains just of two fields (columns)
 */
class Record extends Persistent { 
    String strKey;
    long   intKey;
};

/*
 * This is root object for the storage containing indices for both keys
 */
class Indices extends Persistent {
    Index<Record> strIndex;
    Index<Record> intIndex;
}

public class PerstTest extends Test { 
    final static int nRecords = 10000;
    final static int pagePoolSize = 2*1024*1024;
    
    String databaseName;
    PrintStream out;
    
    PerstTest(String databaseName, PrintStream out) {
    	this.databaseName = databaseName;
    	this.out = out;
    }
    
    public String getName() { 
    	return "Perst";
    }
    
    public void run()
    {
        // Get instance of Perst storage
        Storage db = StorageFactory.getInstance().createStorage();

        // Open the database with given database name and spacified page pool (database cache) size
        db.open(databaseName, pagePoolSize);
        // There is one root object in the database. 
        Indices root = (Indices)db.getRoot();
        if (root == null) {  // if root object was not specified, then storage is not yet initialized
            // Perform initialization:
            // ... create root object
            root = new Indices();
            // ... create indices 
            root.strIndex = db.<Record>createIndex(String.class, true);
            root.intIndex = db.<Record>createIndex(long.class, true);
            // and register new root object
            db.setRoot(root);
        }
        Index<Record> intIndex = root.intIndex;
        Index<Record> strIndex = root.strIndex;
        long start = System.currentTimeMillis();
        long key = 1999;
        int i;   
        // Insert data in the database
        for (i = 0; i < nRecords; i++) { 
            // Perst uses persistence-by-reachability approach: persistent capable class (class derived from
            // org.garret.perst.Persistent) becomes persistent and is stored in the database when it is 
            // referenced from some other persistent object: indices in our case
            Record rec = new Record();
            key = (3141592621L*key + 2718281829L) % 1000000007L;
            rec.intKey = key;
            rec.strKey = Long.toString(key);
            intIndex.put(rec.intKey, rec); // insert record in the index by intKey key
            strIndex.put(rec.strKey, rec); // insert record in the index by sttKey key
        }
        db.commit(); // Commit current transaction
        out.println("Elapsed time for inserting " + nRecords + " records: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");

        start = System.currentTimeMillis();
        key = 1999;
        // This loop perform index searches using both indices
        for (i = 0; i < nRecords; i++) { 
            key = (3141592621L*key + 2718281829L) % 1000000007L;
            Record rec1 = (Record)intIndex.get(key);
            Record rec2 = (Record)strIndex.get(Long.toString(key));
            assert(rec1 != null && rec1 == rec2);
        }
        out.println("Elapsed time for performing " + nRecords*2 + " index searches: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");
 
        start = System.currentTimeMillis();
        key = Long.MIN_VALUE;
        i = 0;
        // Perform iteration through all record using intKey index (records are sorted by intKey)
        for (Record rec : intIndex) { 
            Assert.that(rec.intKey >= key);
            key = rec.intKey;
            i += 1;
        }
        assert(i == nRecords);
        String strKey = "";
        i = 0;
        // Perform iteration through all record using strKey index (records are sorted by strKey)
        for (Record rec : strIndex) { 
            assert(rec.strKey.compareTo(strKey) >= 0);
            strKey = rec.strKey;
            i += 1;
        }
        assert(i == nRecords);
        out.println("Elapsed time for iterating through " + (nRecords*2) + " records: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");

        start = System.currentTimeMillis();
        key = 1999;
        // Locate and remove all records
        for (i = 0; i < nRecords; i++) { 
            key = (3141592621L*key + 2718281829L) % 1000000007L;
            Record rec = (Record)intIndex.get(key);
            intIndex.remove(key, rec); // remove object from intKey index
            strIndex.remove(Long.toString(key)); // remove object from strKey index
            rec.deallocate(); // deallocate object
        }
        // Check that not records are left in the database
        assert(!intIndex.iterator().hasNext());
        assert(!strIndex.iterator().hasNext());
        assert(!intIndex.iterator(null, null, Index.DESCENT_ORDER).hasNext());
        assert(!strIndex.iterator(null, null, Index.DESCENT_ORDER).hasNext());
        assert(!intIndex.iterator(null, null, Index.ASCENT_ORDER).hasNext());
        assert(!strIndex.iterator(null, null, Index.ASCENT_ORDER).hasNext());
        out.println("Elapsed time for deleting " + nRecords + " records: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");
        // Close the database
        db.close();
        out.flush();
        // Notify about test completion
        done();
    }
}
