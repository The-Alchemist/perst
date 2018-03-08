import org.garret.perst.*;

import java.util.Iterator;

public class TestMod { 
    static class Record extends Persistent { 
        String strKey;
        long   intKey;
    };
    
    static class Indices extends Persistent {
        FieldIndex strIndex;
        FieldIndex intIndex;
    }
    
    final static int nRecords = 100000;
    final static int nIterations = 3;
    final static int pagePoolSize = 32*1024*1024;

    static String reverseString(String s) { 
        char[] chars = new char[s.length()];
        for (int i = 0, n = chars.length; i < n; i++) { 
            chars[i] = s.charAt(n-i-1);
        }
        return new String(chars);
    }

    static public void main(String[] args) {    
        Storage db = StorageFactory.getInstance().createStorage();

        if (args.length > 0) { 
            db.setProperty("perst.object.cache.kind", args[0]);
        }
        db.open("testmod.dbs", pagePoolSize);
        Indices root = (Indices)db.getRoot();
        if (root == null) { 
            root = new Indices();
            root.strIndex = db.createFieldIndex(Record.class, "strKey", true);
            root.intIndex = db.createFieldIndex(Record.class, "intKey", true);
            db.setRoot(root);
        }
        FieldIndex intIndex = root.intIndex;
        FieldIndex strIndex = root.strIndex;
        long start = System.currentTimeMillis();
        long key = 1999;
        int i;
        for (i = 0; i < nRecords; i++) { 
            Record rec = new Record();
            key = (3141592621L*key + 2718281829L) % 1000000007L;
            rec.intKey = key;
            rec.strKey = Long.toString(key) + '.';
            intIndex.put(rec);                
            strIndex.put(rec);                
        }
        db.commit();
        System.out.println("Elapsed time for inserting " + nRecords + " records: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");
        


        start = System.currentTimeMillis();
        for (int j = 0; j < nIterations; j++) { 
            key = 1999;
            for (i = 0; i < nRecords; i++) { 
                key = (3141592621L*key + 2718281829L) % 1000000007L;
                Record rec = (Record)intIndex.get(new Key(key));
                strIndex.remove(rec);
                rec.strKey = reverseString(rec.strKey);
                rec.modify();
                strIndex.put(rec);
            }
            System.out.println("Iteration " + j);
            db.commit();
        }           
        System.out.println("Elapsed time for performing " + nRecords*nIterations + " updates: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");
 
        start = System.currentTimeMillis();
        key = 1999;
        for (i = 0; i < nRecords; i++) { 
            key = (3141592621L*key + 2718281829L) % 1000000007L;
            Record rec1 = (Record)intIndex.get(new Key(key));
            Record rec2 = (Record)strIndex.get(reverseString(Long.toString(key) + '.'));
            Assert.that(rec1 != null && rec1 == rec2);
        }
        System.out.println("Elapsed time for performing " + nRecords*2 + " index searches: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");
        
        start = System.currentTimeMillis();
        key = 1999;
        for (i = 0; i < nRecords; i++) { 
            key = (3141592621L*key + 2718281829L) % 1000000007L;
            Record rec = (Record)intIndex.get(new Key(key));
            intIndex.remove(new Key(key));
            strIndex.remove(rec);
            rec.deallocate();
        }
        System.out.println("Elapsed time for deleting " + nRecords + " records: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");
        db.close();
    }
}
