import org.garret.perst.*;

public class TestMap { 
    final static int nRecords = 100000;
    static int pagePoolSize = 32*1024*1024;
    static int smallMapSize = 101;

    public static class Record extends Persistent { 
        String strKey;
        long   intKey;
                
        public void writeObject(IOutputStream out) {
            out.writeString(strKey);
            out.writeLong(intKey);
        }
        
        public void readObject(IInputStream in) {
            strKey = in.readString();
            intKey = in.readLong();
        }
    };
    
    public static class Indices extends Persistent {
        IPersistentMap strMap;
        IPersistentMap intMap;
 
       public void writeObject(IOutputStream out) {
            out.writeObject(strMap);
            out.writeObject(intMap);
        }
        
        public void readObject(IInputStream in) {
            strMap = (IPersistentMap)in.readObject();
            intMap = (IPersistentMap)in.readObject();
        }
    }

    static public void main(String[] args) 
    {    
        Storage db = StorageFactory.getInstance().createStorage();
        db.open("testmap.dbs", pagePoolSize);

        boolean populate = args.length > 0 && args[0].equals("populate");
        Indices root = (Indices)db.getRoot();
        if (root == null) { 
            root = new Indices();
            root.strMap = db.createMap(Types.String, smallMapSize);
            root.intMap = db.createMap(Types.Long, smallMapSize);
            db.setRoot(root);
        }
        Map intMap = root.intMap;
        Map strMap = root.strMap;
        long start = System.currentTimeMillis();
        long key = 1999;
        long checksum = 0;
        int i;        
        if (intMap.size() == 0) { 
            for (i = 0; i < nRecords; i++) { 
                Record rec = new Record();
                key = (3141592621L*key + 2718281829L) % 1000000007L;
                checksum += key;
                rec.intKey = key;
                rec.strKey = Long.toString(key);
                Assert.that(intMap.put(new Long(rec.intKey), rec) == null);                
                Assert.that(strMap.put(rec.strKey, rec) == null); 
                Assert.that(intMap.get(new Long(rec.intKey)) == rec);
                Assert.that(strMap.get(rec.strKey) == rec);
                Assert.that(intMap.put(new Long(rec.intKey), rec) == rec);                
                Assert.that(strMap.put(rec.strKey, rec) == rec); 
                Assert.that(intMap.size() == i+1);
                Assert.that(strMap.size() == i+1);
                if (i+1 == smallMapSize/2) { 
                    iterate(db, i+1, checksum);
                }
            }
            
            db.commit();
            System.out.println("Elapsed time for inserting " + nRecords + " records: " 
                               + (System.currentTimeMillis() - start) + " milliseconds");
        }
        root = (Indices)db.getRoot();
        intMap = root.intMap;
        strMap = root.strMap;
        start = System.currentTimeMillis();
        key = 1999;
        for (i = 0; i < nRecords; i++) { 
            key = (3141592621L*key + 2718281829L) % 1000000007L;
            Record rec1 = (Record)intMap.get(new Long(key));
            Record rec2 = (Record)strMap.get(Long.toString(key));
            Assert.that(rec1 != null && rec1 == rec2);
        }
        System.out.println("Elapsed time for performing " + nRecords*2 + " map searches: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");

        iterate(db, nRecords, checksum);

        if (!populate) { 
            start = System.currentTimeMillis();
            key = 1999;
            for (i = 0; i < nRecords; i++) { 
                key = (3141592621L*key + 2718281829L) % 1000000007L;
                Record rem1 = (Record)intMap.remove(new Long(key));
                Record rem2 = (Record)strMap.remove(Long.toString(key));
                Assert.that(rem1 != null && rem1 == rem2);                
                rem1.deallocate();
            }
            Assert.that(intMap.size() == 0);
            Assert.that(!intMap.iterator().hasNext());
            Assert.that(strMap.size() == 0);
            Assert.that(!strMap.iterator().hasNext());
            System.out.println("Elapsed time for deleting " + nRecords + " records: " 
                               + (System.currentTimeMillis() - start) + " milliseconds");
        }
        db.close();
    }


    static void iterate(Storage db, int nRecords, long checksum)
    {
        long start = System.currentTimeMillis();
        long i, sum;
        Indices root = (Indices)db.getRoot();
        Map intMap = root.intMap;
        Map strMap = root.strMap;
        Iterator iterator = intMap.elements();
        for (i = 0, sum = 0; iterator.hasNext(); i++) { 
            Record rec = (Record)iterator.next();
            sum += rec.intKey;
        }
        Assert.that(i == nRecords && sum == checksum);
        iterator = intMap.keys();
        for (i = 0, sum = 0; iterator.hasNext(); i++) { 
            sum += ((Long)iterator.next()).longValue();
        }
        Assert.that(i == nRecords && sum == checksum);
        iterator = strMap.elements();
        for (i = 0, sum = 0; iterator.hasNext(); i++) { 
            Record rec = (Record)iterator.next();
            sum += rec.intKey;
        }
        Assert.that(i == nRecords && sum == checksum);
        iterator = strMap.keys();
        for (i = 0, sum = 0; iterator.hasNext(); i++) { 
            sum += Long.parseLong((String)iterator.next());
        }
        Assert.that(i == nRecords && sum == checksum);
        iterator = intMap.iterator();
        for (i = 0, sum = 0; iterator.hasNext(); i++) { 
            Map.Entry entry = (Map.Entry)iterator.next();
            Record v = (Record)entry.getValue();
            long k = ((Long)entry.getKey()).longValue();
            Assert.that(k == v.intKey);
            sum += k;
        }
        Assert.that(i == nRecords && sum == checksum);
        System.out.println("Elapsed time for 5 iterations through " + nRecords + " records: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");
    }
}
