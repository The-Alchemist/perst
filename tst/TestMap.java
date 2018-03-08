import org.garret.perst.*;

import java.util.*;


public class TestMap { 
    static int pagePoolSize = 32*1024*1024;

    static class Record extends Persistent { 
        String strKey;
        long   intKey;

        public int compare(Object o) { 
            long key = ((Record)o).intKey;
            return intKey < key ? -1 : intKey == key ? 0 : 1;
        }
    };
    
    static class RecordKey extends Persistent implements Comparable, IValue {
        long   intKey;

        RecordKey() {}
        RecordKey(long val) { intKey = val; }

        public int hashcode() { 
            return (int)intKey ^ (int)(intKey >> 32);
        }

        public boolean equals(Object o) { 
            return o instanceof RecordKey && intKey == ((RecordKey)o).intKey;
        }

        public int compareTo(Object o) { 
            long key = ((RecordKey)o).intKey;
            return intKey < key ? -1 : intKey == key ? 0 : 1;
        }
    };


    static class Indices extends Persistent {
        IPersistentMap strMap;
        IPersistentMap intMap;
        IPersistentMap objMap;
    }

    static public void main(String[] args) {    
        int i;        
        int nRecords = 100000;
        boolean populate = false;
        for (i = 0; i < args.length; i++) { 
            if (args[i].equals("populate")) { 
                populate = true;
            } else if (Character.isDigit(args[i].charAt(0))) { 
                nRecords = Integer.parseInt(args[i]);
            } else { 
                System.err.println("Usage: TestMap.bat [populate] [N_RECORDS]");
                return;
            }
        }

        Storage db = StorageFactory.getInstance().createStorage();
        db.open("testmap.dbs", pagePoolSize);

        Indices root = (Indices)db.getRoot();
        if (root == null) { 
            root = new Indices();
            root.strMap = db.createMap(String.class);
            root.intMap = db.createMap(long.class);
            root.objMap = db.createMap(RecordKey.class);
            db.setRoot(root);
        }
        SortedMap intMap = root.intMap;
        SortedMap strMap = root.strMap;
        IPersistentMap objMap = root.objMap;
        long start = System.currentTimeMillis();
        long key = 1999;
        if (objMap.size() == 0) { 
            for (i = 0; i < nRecords; i++) { 
                Record rec = new Record();
                key = (3141592621L*key + 2718281829L) % 1000000007L;
                rec.intKey = key;
                rec.strKey = Long.toString(key);
                intMap.put(new Long(rec.intKey), rec);                
                strMap.put(rec.strKey, rec); 
                RecordKey rc = new RecordKey();
                rc.intKey = key;
                objMap.put(rc, rec); 
                Assert.that(intMap.get(new Long(rec.intKey)) == rec);
                Assert.that(strMap.get(rec.strKey) == rec);
                Assert.that(objMap.get(rc) == rec);
            }
            db.commit();
            System.out.println("Elapsed time for inserting " + nRecords + " records: " 
                               + (System.currentTimeMillis() - start) + " milliseconds");
        }
        start = System.currentTimeMillis();
        key = 1999;
        for (i = 0; i < nRecords; i++) { 
            key = (3141592621L*key + 2718281829L) % 1000000007L;
            Record rec1 = (Record)intMap.get(new Long(key));
            Record rec2 = (Record)strMap.get(Long.toString(key));
            Record rec3 = (Record)objMap.get(new RecordKey(key));
            Assert.that(rec1 != null && rec1 == rec2 && rec1 == rec3);
        }
        System.out.println("Elapsed time for performing " + nRecords*3 + " map searches: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");

        start = System.currentTimeMillis();
        Iterator iterator = intMap.values().iterator();
        key = Long.MIN_VALUE;
        for (i = 0; iterator.hasNext(); i++) { 
            Record rec = (Record)iterator.next();
            Assert.that(rec.intKey >= key);
            key = rec.intKey;
        }
        Assert.that(i == nRecords);
        iterator = intMap.keySet().iterator();
        key = Long.MIN_VALUE;
        for (i = 0; iterator.hasNext(); i++) { 
            long v = ((Long)iterator.next()).longValue();
            Assert.that(v >= key);
            key = v;
        }
        Assert.that(i == nRecords);
        iterator = strMap.values().iterator();
        String strKey = "";
        for (i = 0; iterator.hasNext(); i++) { 
            Record rec = (Record)iterator.next();
            Assert.that(rec.strKey.compareTo(strKey) >= 0);
            strKey = rec.strKey;
        }
        Assert.that(i == nRecords);
        iterator = strMap.keySet().iterator();
        strKey = "";
        for (i = 0; iterator.hasNext(); i++) { 
            String v = (String)iterator.next();
            Assert.that(v.compareTo(strKey) >= 0);
            strKey = v;
        }
        Assert.that(i == nRecords);
        iterator = objMap.entrySet().iterator();
        key = Long.MIN_VALUE;
        for (i = 0; iterator.hasNext(); i++) { 
            Map.Entry entry = (Map.Entry)iterator.next();
            Record  v = (Record)entry.getValue();
            RecordKey k = (RecordKey)entry.getKey();
            Assert.that(k.intKey == v.intKey);
            Assert.that(k.intKey >= key);
            key = k.intKey;
        }
        Assert.that(i == nRecords);
        System.out.println("Elapsed time for 5 iterations through " + nRecords + " records: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");

        if (!populate) { 
            start = System.currentTimeMillis();
            key = 1999;
            for (i = 0; i < nRecords; i++) { 
                key = (3141592621L*key + 2718281829L) % 1000000007L;
                Record rem1 = (Record)intMap.remove(new Long(key));
                Record rem2 = (Record)strMap.remove(Long.toString(key));
                Map.Entry e = objMap.getEntry(new RecordKey(key));
                Record rem3 = (Record)objMap.remove(e.getKey());
                Assert.that(rem1 != null && rem1 == rem2 && rem1 == rem3 && rem3 == e.getValue());                
                rem1.deallocate();
                ((RecordKey)e.getKey()).deallocate();
            }
            Assert.that(intMap.entrySet().size() == 0);
            Assert.that(!intMap.entrySet().iterator().hasNext());
            Assert.that(strMap.entrySet().size() == 0);
            Assert.that(!strMap.entrySet().iterator().hasNext());
            Assert.that(objMap.entrySet().size() == 0);
            Assert.that(!objMap.entrySet().iterator().hasNext());
            System.out.println("Elapsed time for deleting " + nRecords + " records: " 
                               + (System.currentTimeMillis() - start) + " milliseconds");
        }
        db.close();
    }
}
