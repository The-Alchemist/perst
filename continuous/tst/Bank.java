import org.garret.perst.*;
import org.garret.perst.continuous.*;

import java.util.Random;

class Account extends CVersion
{ 
    @Indexable  
    int id;

    long balance;
}

class Transfer extends CVersion
{  
    CVersionHistory<Account> src;
    CVersionHistory<Account> dst;
    long amount;

    Transfer(Account src, Account dst, long amount) 
    { 
        this.src = src.getVersionHistory();
        this.dst = dst.getVersionHistory();
        this.amount = amount;
    }

    private Transfer() {}
}


public class Bank implements Runnable 
{
    static final int N_ITERATIONS = 1000;
    static final int N_ACCOUNTS = 1000;
    static final int N_THREADS = 10;
    static final int INIT_BALANCE = 100000;

    CDatabase db;
    int nConflicts;

    void initialize() 
    { 
        db.beginTransaction();
        for (int i = 0; i < N_ACCOUNTS; i++) { 
            Account account = new Account();
            account.balance = INIT_BALANCE;
            account.id = i;
            db.insert(account);
        }
        db.commitTransaction();
    }

    void checkBalance() 
    {
        long balance = 0;
        for (Account account : db.<Account>getRecords(Account.class))
        {
            balance += account.balance;
        }
        Assert.that(balance == INIT_BALANCE*N_ACCOUNTS);
    }

    public void run() 
    { 
        Random rand = new Random(Thread.currentThread().hashCode());
        for (int i = 0; i < N_ITERATIONS; i++) { 
            int srcId = rand.nextInt(N_ACCOUNTS);
            int dstId = rand.nextInt(N_ACCOUNTS);
            long amount = rand.nextInt(INIT_BALANCE);
            
            db.beginTransaction();

            Account src = db.getSingleton(db.<Account>find(Account.class, "id", new Key(srcId))).update();
            Account dst = db.getSingleton(db.<Account>find(Account.class, "id", new Key(dstId))).update();
            if (amount > src.balance) { 
                amount = src.balance;
            }
            src.balance -= amount;
            dst.balance += amount;
            
            db.insert(new Transfer(src, dst, amount));
            
            checkBalance();

            try { 
                db.commitTransaction();
            } catch (ConflictException x) { 
                synchronized (this) { 
                    nConflicts += 1;
                }
            }
        }  
    }      

    void runTest() 
    { 
        Storage storage = StorageFactory.getInstance().createStorage();
        storage.setProperty("perst.file.noflush", Boolean.TRUE);
        storage.open("bank.dbs");
        db = new CDatabase();
        db.open(storage, null);
        
        if (!db.getRecords(Account.class).hasNext()) { 
            initialize();
        }

        long start = System.currentTimeMillis();
        Thread[] t = new Thread[N_THREADS];
        for (int i = 0; i < N_THREADS; i++) { 
            t[i] = new Thread(this);
            t[i].start();
        }

        try { 
            for (int i = 0; i < N_THREADS; i++) { 
                t[i].join();
            }
        } catch (InterruptedException x) {}

        long totalTransferred = 0;
        int nTransfers = 0;
        for (Transfer transfer : db.<Transfer>getRecords(Transfer.class)) { 
            nTransfers += 1;
            totalTransferred += transfer.amount;
        }
        Assert.that(nTransfers == N_THREADS*N_ITERATIONS - nConflicts);
        checkBalance();
        System.out.println("Elapsed time for " + N_ITERATIONS + " iterations: " + (System.currentTimeMillis() - start) + " msec");
        System.out.println("Sucessful transactions: " + nTransfers + ", conflicts: " + nConflicts + ", totally tranferred: " + totalTransferred);
        db.close();
    }

    public static void main(String[] args) 
    { 
        new Bank().runTest();
    }
}