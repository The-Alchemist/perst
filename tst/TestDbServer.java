import org.garret.perst.*;
import org.garret.perst.fulltext.*;

import java.util.*;

public class TestDbServer { 
    static class Record extends Persistent { 
        @Indexable(unique=true, caseInsensitive=true)
        String key;
        
        @FullTextIndexable
        String value;
    }
    final static int nThreads = 10;
    final static int nRecords = 10000;

    final static int fullTextSearchMaxResults = 10;
    final static int fullTextSearchTimeout = 10000; // 10 seconds
    
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
                db.beginTransaction();
                Record rec = new Record();
                rec.key = tid + toStr(i) + ".Id";
                rec.value = "Thread" + id + " Key" + i;
                Assert.that(db.addRecord(rec));                
                db.commitTransaction();
            }

            db.beginTransaction();
            i = 0;
            for (Record rec : db.<Record>select(Record.class, "key like '" + tid + "%'")) { 
                Assert.that(rec.key.equals(tid + toStr(i) + ".Id"));
                i += 1;
            }
            Assert.that(i == nRecords);

            FullTextSearchResult result = db.search("thread" + id, null, fullTextSearchMaxResults, fullTextSearchTimeout);
            Assert.that(result.hits.length == 10 && result.estimation == nRecords);

            db.commitTransaction();

            for (i = 0; i < nRecords; i++) { 
                db.beginTransaction();
                String key = tid + toStr(i) + ".ID";
                int n = 0;
                for (Record rec : db.<Record>select(Record.class, "key = '" + key + "'")) {
                    Assert.that(rec.key.equalsIgnoreCase(key));
                    n += 1;
                }
                Assert.that(n == 1);

                result = db.search("Thread" + id + " Key" + i, null, fullTextSearchMaxResults, fullTextSearchTimeout);
                Assert.that(result.hits.length == 1 && result.estimation == 1
                            && ((Record)result.hits[0].getDocument()).key.equalsIgnoreCase(key));
                db.commitTransaction();
            }

            for (i = 0; i < nRecords; i++) { 
                db.beginTransaction();
                String key = tid + toStr(i) + ".id";
                int n = 0;
                for (Record rec : db.<Record>select(Record.class, "key = '" + key + "'", true)) {
                    Assert.that(rec.key.equalsIgnoreCase(key));
                    db.deleteRecord(rec);
                    n += 1;
                    break;
                }
                Assert.that(n == 1);

                db.commitTransaction();
            }
        }
    }

    static public void main(String[] args) throws Exception {    
        Storage storage = StorageFactory.getInstance().createStorage();
        storage.open(new NullFile(), Storage.INFINITE_PAGE_POOL);
        Database db = new Database(storage, true);        

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
