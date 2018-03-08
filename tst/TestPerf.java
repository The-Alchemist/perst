import org.garret.perst.*;

import java.util.*;

public class TestPerf 
{ 
    static class Record extends Persistent
    {
        int intKey;
    }

    static class Root extends Persistent
    {
        FieldIndex<Record> tree;
        IPersistentHash<Integer,Record> hash;        
    }

    static public void main(String[] args)
    {
        int i;
        Storage db = StorageFactory.getInstance().createStorage();
		
        long pagePoolSize = 32 * 1024 * 1024; 
        int  nRecords = 100000;
        int  hashPageSize = 101;
        int  hashLoadFactor = 1;
        String password = "MyPassword";

        for (i = 0; i < args.length; i++) { 
            String opt = args[i];
            if ("inmemory".equals(opt)) { 
                pagePoolSize = Storage.INFINITE_PAGE_POOL;
            } else if ("pool".equals(opt)) { 
                pagePoolSize = Long.parseLong(args[++i]);                                                   
            } else if ("page".equals(opt)) { 
                hashPageSize = Integer.parseInt(args[++i]);
            } else if ("load".equals(opt)) { 
                hashLoadFactor = Integer.parseInt(args[++i]);
            } else if ("altbtree".equals(opt)) { 
                db.setProperty("perst.alternative.btree", Boolean.TRUE);
            } else { 
                System.out.println("Unsupported option");
                return;
            }
        }
        if (pagePoolSize == Storage.INFINITE_PAGE_POOL) {
            db.open(new NullFile(), Storage.INFINITE_PAGE_POOL);
        } else {
            db.open("testidx.dbs", pagePoolSize);
        }
        Root root = (Root)db.getRoot();
        if (root == null) {
            root = new Root();
            root.tree = db.<Record>createFieldIndex(Record.class, "intKey", true);
            root.hash = db.<Integer,Record>createHash(hashPageSize, hashLoadFactor);
            db.setRoot(root);
        }
        FieldIndex<Record> tree = root.tree;
        IPersistentHash<Integer,Record> hash = root.hash;
        long start = System.currentTimeMillis();
        for (i = 0; i < nRecords; i++) {
            Record rec = new Record();
            rec.intKey = i*2;
            tree.put(rec);
            hash.put(rec.intKey, rec);
        }
        db.commit();
        System.out.println("Elapsed time for inserting " + nRecords + " records: " + (System.currentTimeMillis() - start) + " milliseconds");
        start = System.currentTimeMillis();
        for (i = 0; i < nRecords*2; i++) {
            Record rec = tree.get(i);
            if ((i & 1) != 0) {
                Assert.that(rec == null);
            } else {
                Assert.that(rec != null && rec.intKey == i);
            }
        }     
        System.out.println("Elapsed time for performing " + nRecords * 2 + " B-Tree searches: " + (System.currentTimeMillis() - start) + " milliseconds");

        start = System.currentTimeMillis();
        for (i = 0; i < nRecords*2; i++) {
            Record rec = hash.get(i);
            if ((i & 1) != 0) {
                Assert.that(rec == null);
            } else {
                Assert.that(rec != null && rec.intKey == i);
            }
        }     
        System.out.println("Elapsed time for performing " + nRecords * 2 + " hash searches: " + (System.currentTimeMillis() - start) + " milliseconds");

        start = System.currentTimeMillis();
        i = 0;
        for (Record rec: tree) {
            Assert.that(rec.intKey == i*2);
            i += 1;
        }
        Assert.that(i == nRecords);
        System.out.println("Elapsed time for iteration through " + nRecords + " records: " + (System.currentTimeMillis() - start) + " milliseconds");


        start = System.currentTimeMillis();
        for (i = 0; i < nRecords*2; i++)
        {
            Record rec = hash.get(i);
            if ((i & 1) != 0) {
                Assert.that(rec == null);
            } else {
                Assert.that(rec != null && rec.intKey == i);
                tree.remove(rec);
                hash.remove(rec.intKey);             
                rec.deallocate();
            }
        }
        System.out.println("Elapsed time for deleting " + nRecords + " records: " + (System.currentTimeMillis() - start) + " milliseconds");
        db.close();
    }
}
