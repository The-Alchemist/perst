import org.garret.perst.*;


public class TestLoad extends Thread 
{
    static class Record extends Persistent {
        int id;
        byte[] body;
    }

                        
    static final int nElements = 10000;
    static final int nIterations = 100;
    static final int nThreads = 2;
    static final int bodySize = 10000;

    TestLoad(Storage db, int seed) { 
        this.db = db;
        this.seed = seed;
    }

    public void run() { 
        FieldIndex<Record> index = (FieldIndex<Record>)db.getRoot();
        for (int i = 0; i < nIterations; i++) {             
            int j = 0;
            for (Record rec : index) { 
                Assert.that(rec.id == j);
                Assert.that(rec.body.length == bodySize);
                j += 1;
            }
            Assert.that(j == nElements);
            long key = 1999 + seed;
            for (int k = 0; k < nElements; k++) {
                key = (3141592621L*key + 2718281829L) % 1000000007L;
                int id = (int)(key % nElements);
                Record rec = index.get(id);
                Assert.that(rec.id == id);
            }
        }
    }

    public static void main(String[] args) throws Exception { 
        Storage db = StorageFactory.getInstance().createStorage();
        db.setProperty("perst.object.cache.init.size", 10);
        db.open("testload.dbs");
        FieldIndex<Record> index = (FieldIndex<Record>)db.getRoot();
        if (index == null) { 
            index = db.<Record>createFieldIndex(Record.class, "id", true);
            db.setRoot(index);
            for (int i = 0; i < nElements; i++) { 
                Record rec = new Record();
                rec.id = i;
                rec.body = new byte[bodySize];
                index.add(rec);                
            }
            db.commit();
            System.out.println("Intialization completed");
        }
        Thread[] threads = new TestLoad[nThreads];
        for (int i = 0; i < nThreads; i++) { 
            threads[i] = new TestLoad(db, i);
            threads[i].start();
        }
        for (int i = 0; i < nThreads; i++) { 
            threads[i].join();
        }
        db.close();
    }
        
    Storage db;
    int seed;
}
