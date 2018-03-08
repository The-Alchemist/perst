import org.garret.perst.*;

import java.util.*;

public class TestKDTree 
{ 
    static class Quote extends Persistent
    { 
        int    timestamp;
        float  low;
        float  high;
        float  open;
        float  close;
        int    volume;
        
        boolean eq(Quote q) { 
            return low == q.low && high == q.high && open == q.open && close == q.close && volume == q.volume;
        }
        
        boolean le(Quote q) { 
            return low <= q.low && high <= q.high && open <= q.open && close <= q.close && volume <= q.volume;
        }
    }

    final static int nRecords = 100000;
    static final int pagePoolSize = 32*1024*1024;
    static final int MAX_PRICE = 100;
    static final int MAX_VOLUME = 10000;
    static final int EPSILON = 100;
    static final int KD_TREE_OPTIMIZATION_THRESHOLD = 2;

    public static Quote getRandomQuote(Random r) { 
        Quote q = new Quote();
        q.timestamp = (int)(System.currentTimeMillis()/1000);
        q.low = (float)r.nextInt(MAX_PRICE*10)/10;
        q.high = q.low + (float)r.nextInt(MAX_PRICE*10)/10;
        q.open = (float)r.nextInt(MAX_PRICE*10)/10;
        q.close = (float)r.nextInt(MAX_PRICE*10)/10;
        q.volume = r.nextInt(MAX_VOLUME);
        return q;
    }
    
    static public void main(String[] args) {    
        Storage db = StorageFactory.getInstance().createStorage();
        db.open("testkbtree.dbs", pagePoolSize);
        boolean populate = args.length > 0 && args[0].equals("populate");

        MultidimensionalIndex index = (MultidimensionalIndex)db.getRoot();
        if (index == null) { 
            index = db.createMultidimensionalIndex(Quote.class, new String[] { "low", "high", "open", "close", "volume" }, false);
            db.setRoot(index);
        } 
        Random r;
        long start;
        if (index.size() == 0) { 
            r = new Random(2007); 
            start = System.currentTimeMillis();
            for (int i = 0; i < nRecords; i++) { 
                Quote q = getRandomQuote(r);
                index.add(q);
            }
            db.commit();
            System.out.println("Elapsed time for inserting " + nRecords + " records: " 
                               + (System.currentTimeMillis() - start) + " milliseconds");

            System.out.println("Tree height: " + index.getHeight());
            if (index.size() > 1 && index.getHeight()/Math.log(index.size())*Math.log(2.0) > KD_TREE_OPTIMIZATION_THRESHOLD) { 
                start = System.currentTimeMillis();
                index.optimize();
                System.out.println("New tree height: " + index.getHeight());
                System.out.println("Elapsed time for tree optimization: " + (System.currentTimeMillis() - start) + " milliseconds");
            }
        }
        start = System.currentTimeMillis();
        r = new Random(2007);
        long total = 0;
        for (int i = 0; i < nRecords; i++) {
            Quote q = getRandomQuote(r);            
            IPersistent[] result = index.queryByExample(q);
            Assert.that(result.length >= 1);
            total += result.length;
            for (int j = 0; j < result.length; j++) { 
                Assert.that(q.eq((Quote)result[j]));
            }
        }
        System.out.println("Elapsed time for performing " + nRecords + " query by example searches: " 
                           + (System.currentTimeMillis() - start) + " milliseconds, select " + total + " object");
 
        start = System.currentTimeMillis();
        r = new Random(2007);
        Random r2 = new Random(2008);
        total = 0;
        for (int i = 0; i < nRecords; i++) {
            Quote q = getRandomQuote(r);            
            Quote min = new Quote();
            Quote max = new Quote();

            min.low = q.low - (float)MAX_PRICE/EPSILON;
            min.high = q.high - (float)MAX_PRICE/EPSILON;
            min.open = q.open - (float)MAX_PRICE/EPSILON;
            min.close = q.close - (float)MAX_PRICE/EPSILON;
            min.volume = q.volume - MAX_VOLUME/EPSILON;

            max.low = q.low + (float)MAX_PRICE/EPSILON;
            max.high = q.high + (float)MAX_PRICE/EPSILON;
            max.open = q.open + (float)MAX_PRICE/EPSILON;
            max.close = q.close + (float)MAX_PRICE/EPSILON;
            max.volume = q.volume + MAX_VOLUME/EPSILON;

            Iterator iterator = index.iterator(min, max);
            int n = 0;
            while (iterator.hasNext()) { 
                q = (Quote)iterator.next();
                Assert.that(min.le(q));
                Assert.that(q.le(max));
                n += 1;
            }
            Assert.that(n >= 1);
            total += n;
        }
        System.out.println("Elapsed time for performing " + nRecords + " range query by example searches: " 
                           + (System.currentTimeMillis() - start) + " milliseconds, select " + total + " objects");


        start = System.currentTimeMillis();
        Iterator iterator = index.iterator();
        int n = 0;
        while (iterator.hasNext()) { 
            iterator.next();
            n += 1;
        }
        Assert.that(n == nRecords);
        System.out.println("Elapsed time for iterating through " + nRecords + " records: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");

        if (!populate) {
            start = System.currentTimeMillis();
            iterator = index.iterator();
            n = 0;
            while (iterator.hasNext()) { 
                Quote q = (Quote)iterator.next();
                iterator.remove();
                q.deallocate();
                n += 1;
            }
            db.commit();
            Assert.that(n == nRecords);
            System.out.println("Elapsed time for removing " + nRecords + " records: " 
                               + (System.currentTimeMillis() - start) + " milliseconds");            
            Assert.that(!index.iterator().hasNext());
            index.clear();
            Assert.that(index.size() == 0);
        }
        db.close();
    }
}
