import org.garret.perst.*;

import java.util.*;

public class TestServer { 
    static class Record extends Persistent { 
        String key;
    }
    static class Root extends Persistent { 
        FieldIndex[] indices;
    }
    
    final static int nThreads = 10;
    final static int nIndices = 10;
    final static int nRecords = 10000;

    static String toStr(int i) { 
        String s = "000000" + i;
        return s.substring(s.length()-6);
    }

    static class ClientThread extends Thread {
        Storage db;
        int     id;
        
        ClientThread(Storage db, int id) {
            this.db = db;
            this.id = id;
            start();
        }

        public void run() { 
            int i;
            Root root = (Root)db.getRoot();
            String tid = "Thread" + id + ":";
            FieldIndex index = root.indices[id % nIndices];

            for (i = 0; i < nRecords; i++) { 
                db.beginThreadTransaction(Storage.SERIALIZABLE_TRANSACTION);
                index.exclusiveLock();
                Record rec = new Record();
                rec.key = tid + toStr(i);
                index.put(rec);
                db.endThreadTransaction();
            }

            index.sharedLock();
            Iterator iterator = index.prefixIterator(tid);
            for (i = 0; iterator.hasNext(); i++) { 
                Record rec = (Record)iterator.next();
                Assert.that(rec.key.equals(tid + toStr(i)));
            }
            Assert.that(i == nRecords);
            index.unlock();

            for (i = 0; i < nRecords; i++) { 
                index.sharedLock();
                String key = tid + toStr(i);
                Record rec = (Record)index.get(key);
                Assert.that(rec.key.equals(key));
                index.unlock();
            }

            for (i = 0; i < nRecords; i++) { 
                db.beginThreadTransaction(Storage.SERIALIZABLE_TRANSACTION);
                index.exclusiveLock();
                Record rec = (Record)index.remove(new Key(tid + toStr(i)));
                rec.deallocate();
                db.endThreadTransaction();
            }
        }
    }

    static public void main(String[] args) throws Exception {    
        Storage db = StorageFactory.getInstance().createStorage();
        db.setProperty("perst.alternative.btree", Boolean.TRUE);
        db.open(new NullFile(), Storage.INFINITE_PAGE_POOL);
        Root root = (Root)db.getRoot();
        if (root == null) { 
            root = new Root();
            root.indices = new FieldIndex[nIndices];
            for (int i = 0; i < nIndices; i++) {
                root.indices[i] = db.createFieldIndex(Record.class, "key", true);
            }
            db.setRoot(root);
        }        
        long start = System.currentTimeMillis();
        Thread[] threads = new Thread[nThreads];
        for (int i = 0; i < nThreads; i++) { 
            threads[i] = new ClientThread(db, i);
        }
        for (int i = 0; i < nThreads; i++) { 
            threads[i].join();
        }
        db.close();
        System.out.println("Elapsed time: " + (System.currentTimeMillis() - start) + " milliseconds");
    }
}
