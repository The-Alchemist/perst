import org.garret.perst.*;

import java.util.*;

public class TestRegex
{ 
    final static int nRecords = 1024*1024;

    static class Record extends Persistent { 
        String key;
    };

    static public void main(String[] args) 
    {    
        Storage db = StorageFactory.getInstance().createStorage();
        int pagePoolSize = 256*1024*1024;
        db.open("testregex.dbs", pagePoolSize);

        RegexIndex<Record> index = db.<RegexIndex<Record>>getRoot();
        if (index == null) { 
            index = db.<Record>createRegexIndex(Record.class, "key");
            db.setRoot(index);
        }
        long start = System.currentTimeMillis();
        for (int i = 0; i < nRecords; i++) { 
            Record rec = new Record();
            rec.key = Integer.toHexString(i);
            index.add(rec);
        }
        db.commit();
        System.out.println("Elapsed time for inserting " + nRecords + " records: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");

        start = System.currentTimeMillis();
        int n = 0;
        for (Record rec : index.match("%Abcd%")) { 
            n += 1;
            Assert.that(rec.key.indexOf("abcd") >= 0);
        }
        System.out.println("Elapsed time for query LIKE '%abcd%': "  + (System.currentTimeMillis() - start) + " milliseconds");
        Assert.that(n == 16*2);
        
        start = System.currentTimeMillis();
        n = 0;
        for (Record rec : index.match("1_2_3")) { 
            n += 1;
        }
        System.out.println("Elapsed time for query LIKE '1_2_3': "  + (System.currentTimeMillis() - start) + " milliseconds");
        Assert.that(n == 16*16);
        
        start = System.currentTimeMillis();
        Iterator<Record> iterator = index.iterator();
        while (iterator.hasNext()) { 
            Record rec = iterator.next();
            iterator.remove();
            rec.deallocate();
        }
        Assert.that(!index.iterator().hasNext());
        System.out.println("Elapsed time for deleting " + nRecords + " records: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");
        db.close();
    }
}
