import org.garret.perst.*;

import java.util.*;

public class TestRecovery { 
    static class Indices extends Persistent {
        IPersistentSet set;
        Index          index;
    }

    static class Record extends Persistent { 
        int id;

        Record() {}
        Record(int id) { this.id = id; }
    }

    final static int crashPeriod = 10;
    final static int maxNumberOfLostUpdates = 10;
    static int pagePoolSize = 32*1024*1024;
    
    static public void main(String[] args) {    
        int i, j;
        Storage db = StorageFactory.getInstance().createStorage();
        db.open("testrecovery.dbs", pagePoolSize);

        Indices root = (Indices)db.getRoot();
        if (root == null) { 
            root = new Indices();
            root.set = db.createSet();
            root.index = db.createIndex(int.class, true);
            db.setRoot(root);
        }
        int n = root.set.size();
        System.out.println("Size " + n);
        Assert.that(n == root.index.size());
        Assert.that(n % crashPeriod == 0);
        for (i = 0; i < n; i++) { 
            Record rec = (Record)root.index.get(new Key(i));
            Assert.that(rec.id == i);
        }
        Iterator iterator = root.set.iterator();
        int sum = 0;
        for (i = 0; iterator.hasNext(); i++) {             
            Record rec = (Record)iterator.next();
            sum += rec.id;
        }
        Assert.that(i == n);
        Assert.that(sum == (long)i*(i-1)/2);
        
        for (j = 0; j < crashPeriod; i++, j++) { 
            Record rec = new Record(i);
            root.set.add(rec);
            root.index.put(new Key(i), rec);
        }
        db.commit();
        for (j = i % maxNumberOfLostUpdates; --j >= 0; i++) { 
            Record rec = new Record(i);
            root.set.add(rec);
            root.index.put(new Key(i), rec);
        }                                       
        throw new RuntimeException("crashed");
    }
}
