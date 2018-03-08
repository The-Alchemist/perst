import org.garret.perst.*;

import java.util.*;

public class TestReplic2 { 
    static class Record extends Persistent { 
        int key;
    }
    
    final static int nIterations = 100000;
    final static int nRecords = 1000;
    final static int transSize = 100;
    final static int defaultPort = 6000;
    final static int asyncBufSize = 1024*1024;

    private static void usage() { 
        System.err.println("Usage: java TestReplic (master|slave) [port] [-async] [-ack] [-db DATABASE_FILE] [-map MAP_FILE]");
    }

    static public void main(String[] args) {    
        int i;
        if (args.length < 1) {
            usage();
            return;
        }
        int port = defaultPort;
        boolean ack = false;
        boolean async = false;
        String mapFile = null;
        String dbFile = null;
        for (i = 1; i < args.length; i++) { 
            if (args[i].startsWith("-")) { 
                if (args[i].equals("-async")) { 
                    async = true;
                } else if (args[i].equals("-ack")) { 
                    ack = true;
                } else if (args[i].equals("-map")) { 
                    mapFile = args[++i];
                } else if (args[i].equals("-db")) { 
                    dbFile = args[++i];
                } else { 
                    usage();
                }
            } else { 
                port = Integer.parseInt(args[i]);
            }
        }
        if ("master".equals(args[0])) { 
            ReplicationMasterStorage db = 
                StorageFactory.getInstance().createReplicationMasterStorage(port, new String[0],
                                                                            async ? asyncBufSize : 0, mapFile);
            db.setProperty("perst.replication.ack", Boolean.valueOf(ack));
            if (dbFile == null) { 
                db.open(new NullFile(), Storage.INFINITE_PAGE_POOL);
            } else { 
                db.open(dbFile);
            }

            FieldIndex<Record> root = (FieldIndex<Record>)db.getRoot();
            if (root == null) { 
                root = db.<Record>createFieldIndex(Record.class, "key", true);
                db.setRoot(root);
            }
            long start = System.currentTimeMillis();
            for (i = 0; i < nIterations; i++) {
                if (i >= nRecords) { 
                    Record obj = root.remove(new Key(i-nRecords));
                    obj.deallocate();
                }
                Record rec = new Record();
                rec.key = i;
                root.put(rec);
                if (i >= nRecords && i % transSize == 0) {
                    db.commit();
                }
            }
            db.close();
            System.out.println("Elapsed time for " + nIterations + " iterations: " 
                               + (System.currentTimeMillis() - start) + " milliseconds");
        } else if ("slave".equals(args[0])) { 
            ReplicationSlaveStorage db = 
                StorageFactory.getInstance().addReplicationSlaveStorage("localhost", port, mapFile); 
            db.setProperty("perst.replication.ack", Boolean.valueOf(ack));
            if (dbFile == null) { 
                db.open(new NullFile(), Storage.INFINITE_PAGE_POOL);         
            } else { 
                db.open(dbFile);
            }
            long total = 0;
            int n = 0;
            while (db.isConnected()) { 
                db.waitForModification();
                db.beginThreadTransaction(Storage.REPLICATION_SLAVE_TRANSACTION);
                FieldIndex<Record> root = (FieldIndex<Record>)db.getRoot();
                if (root != null && root.size() == nRecords) {
                    long start = System.currentTimeMillis();
                    Iterator<Record> iterator = root.iterator();
                    int prevKey = iterator.next().key;
                    System.out.println("First key: " + prevKey);
                    for (i = 1; iterator.hasNext(); i++) { 
                        int key = iterator.next().key;
                        Assert.that(key == prevKey+1);
                        prevKey = key;
                    }
                    Assert.that(i == nRecords);
                    n += 1;
                    total += (System.currentTimeMillis() - start); 
                }
                db.endThreadTransaction();
            }
            db.close();
            System.out.println("Elapsed time for " + n + " iterations: " + total + " milliseconds");
        } else {
            usage();
        }
    }
}
