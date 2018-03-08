import org.garret.perst.*;

import java.util.*;

public class TestRndIndex { 
    static class Record extends Persistent { 
        int i;
    };

    final static int nRecords = 100003; // should be prime
    final static int step = 5;
    static int pagePoolSize = 32*1024*1024;

    static public void main(String[] args) {    
        Storage db = StorageFactory.getInstance().createStorage();
        db.open("testrnd.dbs", pagePoolSize);
            
        FieldIndex root = (FieldIndex)db.getRoot();
        if (root == null) { 
            root = db.createRandomAccessFieldIndex(Record.class, "i", true);
            db.setRoot(root);
        }
        long start = System.currentTimeMillis();
        int i, j;        
        for (i = 0, j = 0; i < nRecords; i++, j += step) { 
            Record rec = new Record();
            rec.i = j % nRecords;
            root.put(rec);
        }
        db.commit();
        //db.gc();
        System.out.println("Elapsed time for inserting " + nRecords + " records: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");

        start = System.currentTimeMillis();
        for (i = 0; i < nRecords; i++) { 
            Record rec = (Record)root.get(new Key(i));
            Assert.that(rec.i == i);
            Assert.that(root.indexOf(new Key(i)) == i);
        }
        System.out.println("Elapsed time for performing " + nRecords + " get operations: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");

        start = System.currentTimeMillis();
        for (i = 0; i < nRecords; i++) { 
            Record rec = (Record)root.getAt(i);
            Assert.that(rec.i == i);
        }
        System.out.println("Elapsed time for performing " + nRecords + " getAt operations: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");

        start = System.currentTimeMillis();
        Iterator iterator = root.entryIterator(nRecords/2, Index.ASCENT_ORDER);
        for (i = nRecords/2; iterator.hasNext(); i++) { 
            Map.Entry e = (Map.Entry)iterator.next();
            Assert.that(((Integer)e.getKey()).intValue() == i && ((Record)e.getValue()).i == i);
        }
        Assert.that(i == nRecords);
        iterator = root.entryIterator(nRecords/2-1, Index.DESCENT_ORDER);
        for (i = nRecords/2-1; iterator.hasNext(); i--) { 
            Map.Entry e = (Map.Entry)iterator.next();
            Assert.that(((Integer)e.getKey()).intValue() == i && ((Record)e.getValue()).i == i);
        }
        Assert.that(i == -1);
        System.out.println("Elapsed time for iterating through " + nRecords + " records: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");

        
        start = System.currentTimeMillis();
        for (i = 0, j = 0; i < nRecords; i += step, j++) { 
            Record rec = (Record)root.getAt(i-j);
            Assert.that(rec.i == i);
            root.remove(rec);
            rec.deallocate();
        }
        System.out.println("Elapsed time for deleting " + nRecords/step + " records: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");
        root.clear();
        Assert.that(!root.iterator().hasNext());
        db.close();
    }
}
