import org.garret.perst.*;

import java.util.*;

public class TestJSQL { 
    static class Record extends Persistent { 
        String strKey;
        long   intKey;
        Date   dateKey;
    };
    
    static class Indices extends Persistent {
        FieldIndex strIndex;
        FieldIndex intIndex;
        FieldIndex dateIndex;
    }

    final static int nRecords = 100000;
    static int pagePoolSize = 32*1024*1024;

    static public void main(String[] args) {    
        Storage db = StorageFactory.getInstance().createStorage();
        db.open("testjsql.dbs", pagePoolSize);

        Indices root = (Indices)db.getRoot();
        if (root == null) { 
            root = new Indices();
            root.strIndex = db.createFieldIndex(Record.class, "strKey", true);
            root.intIndex = db.createFieldIndex(Record.class, "intKey", true);
            root.dateIndex = db.createFieldIndex(Record.class, "dateKey", false);
            db.setRoot(root);
        }
        FieldIndex intIndex = root.intIndex;
        FieldIndex strIndex = root.strIndex;
        FieldIndex dateIndex = root.dateIndex;
        long start = System.currentTimeMillis();
        long begin = start;
        long key = 1999;
        Iterator iterator;
        int i;        
        for (i = 0; i < nRecords; i++) { 
            Record rec = new Record();
            key = (3141592621L*key + 2718281829L) % 1000000007L;
            rec.intKey = key;
            rec.strKey = Long.toString(key);
            rec.dateKey = new Date();
            intIndex.put(rec);                
            strIndex.put(rec);           
            dateIndex.put(rec);                
        }
        db.commit();
        long end = System.currentTimeMillis();
        System.out.println("Elapsed time for inserting " + nRecords + " records: " 
                           + (end - start) + " milliseconds");

        start = System.currentTimeMillis();
        key = 1999;
        Query q1 = db.createQuery();
        q1.prepare(Record.class, "strKey=?");        
        q1.addIndex("strKey", strIndex);

        Query q2 = db.createQuery();
        q2.addIndex("intKey", intIndex);
        q2.prepare(Record.class, "intKey=?");        

        Query q3 = db.createQuery();
        q3.addIndex("dateKey", dateIndex);
        q3.prepare(Record.class, "dateKey between ? and ?");        

        for (i = 0; i < nRecords; i++) { 
            key = (3141592621L*key + 2718281829L) % 1000000007L;
            q1.setParameter(1, Long.toString(key));
            iterator = q1.execute(intIndex.iterator());
            Record rec1 = (Record)iterator.next();
            Assert.that(!iterator.hasNext());

            q2.setIntParameter(1, key);
            iterator = q2.execute(strIndex.iterator());
            Record rec2 = (Record)iterator.next();
            Assert.that(!iterator.hasNext());

            Assert.that(rec1 != null && rec1 == rec2);
        }
        System.out.println("Elapsed time for performing " + nRecords*2 + " index searches: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");
 
        start = System.currentTimeMillis();
        iterator = intIndex.select("strKey=string(intKey)");
        key = Long.MIN_VALUE;
        for (i = 0; iterator.hasNext(); i++) { 
            Record rec = (Record)iterator.next();
            Assert.that(rec.intKey >= key);
            key = rec.intKey;
        }
        Assert.that(i == nRecords);
        System.out.println("Elapsed time for iterating through " + nRecords + " records: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");

        start = System.currentTimeMillis();
        iterator = strIndex.select("(intKey and 1023) = 0 order by intKey");
        key = Long.MIN_VALUE;
        for (i = 0; iterator.hasNext(); i++) { 
            Record rec = (Record)iterator.next();
            Assert.that(rec.intKey >= key);
            key = rec.intKey;
        }
        System.out.println("Elapsed time for ordering " + i + " records: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");


        start = System.currentTimeMillis();
        iterator = intIndex.select("strKey=string(intKey)");
        key = Long.MIN_VALUE;
        for (i = 0; iterator.hasNext(); i++) { 
            Record rec = (Record)iterator.next();
            Assert.that(rec.intKey >= key);
            key = rec.intKey;
        }
        Assert.that(i == nRecords);
        System.out.println("Elapsed time for iterating through " + nRecords + " records: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");


        start = System.currentTimeMillis();
        q3.setParameter(1, new Date(begin));
        q3.setParameter(2, new Date(end));
        iterator = q3.execute(dateIndex.iterator());
        long curr = begin;
        i = 0;
        while (iterator.hasNext()) { 
            Record rec = (Record)iterator.next();
            Assert.that(rec.dateKey.getTime() >= curr);
            curr = rec.dateKey.getTime();
            i += 1;
        }
        System.out.println("Elapsed time for iterating through date index for " + i + " records: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");
        Assert.that(i == nRecords);

        start = System.currentTimeMillis();
        iterator = intIndex.iterator(null, null, Index.ASCENT_ORDER);
        while (iterator.hasNext()) { 
            Record rec = (Record)iterator.next();
            rec.deallocate();
        }
        intIndex.deallocate();
        strIndex.deallocate();
        dateIndex.deallocate();
        System.out.println("Elapsed time for deleting all data: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");
        db.close();
    }
}
