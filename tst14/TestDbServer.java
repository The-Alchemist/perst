import org.garret.perst.*;

import java.util.*;

public class TestDbServer { 
    static class Record extends Persistent { 
        String key;
    }
    final static int nThreads = 10;
    final static int nRecords = 10000;

    static String toStr(int i) { 
        String s = "000000" + i;
        return s.substring(s.length()-6);
    }

    static class ClientThread extends Thread {
        Database db;
        int      id;
        
        ClientThread(Database db, int id) {
            this.db = db;
            this.id = id;
            start();
        }

        public void run() { 
            int i;
            String tid = "Thread" + id + ":";
            Storage storage = db.getStorage();

            for (i = 0; i < nRecords; i++) { 
                storage.beginThreadTransaction(Storage.SERIALIZABLE_TRANSACTION);
                Record rec = new Record();
                rec.key = tid + toStr(i);
                db.addRecord(rec);
                storage.endThreadTransaction();
            }

            storage.beginThreadTransaction(Storage.SERIALIZABLE_TRANSACTION);
            Iterator iterator = db.select(Record.class, "key like '" + tid + "%'");
            for (i = 0; iterator.hasNext(); i++) { 
                Record rec = (Record)iterator.next();
                Assert.that(rec.key.equals(tid + toStr(i)));
            }
            Assert.that(i == nRecords);
            storage.endThreadTransaction();

            for (i = 0; i < nRecords; i++) { 
                storage.beginThreadTransaction(Storage.SERIALIZABLE_TRANSACTION);
                String key = tid + toStr(i);
                iterator = db.select(Record.class, "key = '" + key + "'");
                Record rec = (Record)iterator.next();
                Assert.that(rec.key.equals(key));
                Assert.that(!iterator.hasNext());
                storage.endThreadTransaction();
            }

            for (i = 0; i < nRecords; i++) { 
                storage.beginThreadTransaction(Storage.SERIALIZABLE_TRANSACTION);
                String key = tid + toStr(i);
                iterator = db.select(Record.class, "key = '" + key + "'", true);
                Record rec = (Record)iterator.next();
                Assert.that(rec.key.equals(key));
                Assert.that(!iterator.hasNext());
                db.deleteRecord(rec);
                storage.endThreadTransaction();
            }
        }
    }

    static public void main(String[] args) throws Exception {    
        Storage storage = StorageFactory.getInstance().createStorage();
        storage.setProperty("perst.alternative.btree", Boolean.TRUE);
        storage.open(new NullFile(), Storage.INFINITE_PAGE_POOL);
        Database db = new Database(storage, true);        

        storage.beginThreadTransaction(Storage.SERIALIZABLE_TRANSACTION);
        db.createTable(Record.class);
        db.createIndex(Record.class, "key", true);
        storage.endThreadTransaction();
        
        long start = System.currentTimeMillis();
        Thread[] threads = new Thread[nThreads];
        for (int i = 0; i < nThreads; i++) { 
            threads[i] = new ClientThread(db, i);
        }
        for (int i = 0; i < nThreads; i++) { 
            threads[i].join();
        }
        storage.close();
        System.out.println("Elapsed time: " + (System.currentTimeMillis() - start) + " milliseconds");
    }
}
