import org.garret.perst.*;

import java.util.*;

public class Benchmark { 
    static class Record extends Persistent { 
        int intKey;
    };

    static class Indices extends Persistent {
        Index intIndex;
    }

    final static int nRecords = 100000;
    static int pagePoolSize = 64*1024*1024;

    static public void main(String[] args) {    
        Storage db = StorageFactory.getInstance().createStorage();
        boolean serializableTransaction = false;
        for (int i = 0; i < args.length; i++) { 
            if ("inmemory".equals(args[i])) { 
                pagePoolSize = Storage.INFINITE_PAGE_POOL;
            } else if ("altbtree".equals(args[i])) { 
                db.setProperty("perst.alternative.btree", Boolean.TRUE);
                //db.setProperty("perst.object.cache.kind", "weak");
                db.setProperty("perst.object.cache.init.size", new Integer(1013));
            } else if ("serializable".equals(args[i])) { 
                db.setProperty("perst.alternative.btree", Boolean.TRUE);
                serializableTransaction = true;
            } else if ("gc".equals(args[i])) { 
                db.setProperty("perst.gc.threshold", new Integer(1024*1024));
                db.setProperty("perst.background.gc", Boolean.TRUE);
            } else { 
                System.err.println("Unrecognized option: " + args[i]);
            }
        }
        //db.open("@benchmark.mdf", pagePoolSize); // multifile
        if (pagePoolSize == Storage.INFINITE_PAGE_POOL) { 
            db.open(new NullFile(), pagePoolSize);
        } else { 
            db.open("benchmark.dbs", pagePoolSize);
        }

        if (serializableTransaction) { 
            db.beginThreadTransaction(Storage.SERIALIZABLE_TRANSACTION);
        }
            
        Indices root = (Indices)db.getRoot();
        if (root == null) { 
            root = new Indices();
            root.intIndex = db.createIndex(int.class, true);
            db.setRoot(root);
        }
        Index intIndex = root.intIndex;
        long start = System.currentTimeMillis();
        int i;        
        for (i = 0; i < nRecords; i++) { 
            Record rec = new Record();
            rec.intKey = i;
            intIndex.put(new Key(rec.intKey), rec);                
        }
        
        if (serializableTransaction) { 
            db.endThreadTransaction();
            db.beginThreadTransaction(Storage.SERIALIZABLE_TRANSACTION);
        } else { 
            db.commit();
        }
        //db.gc();
        System.out.println("Elapsed time for inserting " + nRecords + " records: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");

        start = System.currentTimeMillis();
        for (i = 0; i < nRecords; i++) { 
            Record rec = (Record)intIndex.get(new Key(i));
            Assert.that(rec != null && rec.intKey == i);
        }
        System.out.println("Elapsed time for performing " + nRecords + " index searches: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");
 
        start = System.currentTimeMillis();
        Iterator iterator = intIndex.iterator();
        for (i = 0; iterator.hasNext(); i++) { 
            Record rec = (Record)iterator.next();
            Assert.that(rec.intKey == i);
        }
        Assert.that(i == nRecords);
        System.out.println("Elapsed time for iterating through " + nRecords + " records: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");

        start = System.currentTimeMillis();
        for (i = 0; i < nRecords; i++) { 
            Record rec = (Record)intIndex.get(new Key(i));
            Record removed = (Record)intIndex.remove(new Key(i));
            Assert.that(removed == rec);
            rec.deallocate();
        }
        Assert.that(!intIndex.iterator().hasNext());
        Assert.that(!intIndex.iterator(null, null, Index.DESCENT_ORDER).hasNext());
        Assert.that(!intIndex.iterator(null, null, Index.ASCENT_ORDER).hasNext());
        System.out.println("Elapsed time for deleting " + nRecords + " records: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");
        db.close();
    }
}
