import org.garret.perst.*;

import java.util.*;

public class TestList { 
    static class Record extends Persistent { 
        int i;
    };

    final static int nRecords = 100000;
    static int pagePoolSize = 32*1024*1024;

    static public void main(String[] args) {    
        Storage db = StorageFactory.getInstance().createStorage();
        db.open("testlist.dbs", pagePoolSize);
            
        IPersistentList root = (IPersistentList)db.getRoot();
        if (root == null) { 
            root = db.createList();
            db.setRoot(root);
        }
        long start = System.currentTimeMillis();
        int i;        
        for (i = 0; i < nRecords/2; i++) { 
            Record rec = new Record();
            rec.i = i*2;
            root.add(rec);
        }
        for (i = 1; i < nRecords; i+=2) { 
            Record rec = new Record();
            rec.i = i;
            root.add(i, rec);
        }

        db.commit();
        //db.gc();
        System.out.println("Elapsed time for inserting " + nRecords + " records: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");

        start = System.currentTimeMillis();
        for (i = 0; i < nRecords; i++) { 
            Record rec = (Record)root.get(i);
            Assert.that(rec.i == i);
        }
        System.out.println("Elapsed time for performing " + nRecords + " gets: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");
 
        start = System.currentTimeMillis();
        Iterator iterator = root.iterator();
        for (i = 0; iterator.hasNext(); i++) { 
            Record rec = (Record)iterator.next();
            Assert.that(rec.i == i);
        }
        Assert.that(i == nRecords);
        System.out.println("Elapsed time for iterating through " + nRecords + " records: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");

        
        start = System.currentTimeMillis();
        /*
        for (i = nRecords; --i >= 0;) {
            Record rec = (Record)root.remove(i); 
            Assert.that(rec.i == i);
            rec.deallocate();
        }
        /**/
        for (i = 0; i < nRecords; i++) { 
            Record rec = (Record)root.remove(0);
            Assert.that(rec.i == i);
            rec.deallocate();
        }
        /**/
        Assert.that(!root.iterator().hasNext());
        System.out.println("Elapsed time for deleting " + nRecords + " records: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");
        db.close();
    }
}
