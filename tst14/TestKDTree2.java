import org.garret.perst.*;

import java.util.*;

public class TestKDTree2 
{ 
    static class Stock extends Persistent { 
        String symbol;
        float  price;
        int    volume;

        boolean eq(Stock s) { 
            return symbol.equals(s.symbol) && price == s.price && volume == s.volume;
        }
        
        boolean le(Stock s) { 
            return price <= s.price && volume <= s.volume;
        }
    };
    
    static class StockComparator extends MultidimensionalComparator
    {
        public int compare(Object m1, Object m2, int i) { 
            Stock s1 = (Stock)m1;
            Stock s2 = (Stock)m2;
            switch (i) { 
            case 0:
                if (s1.symbol == null && s2.symbol == null) { 
                    return EQ;
                } else if (s1.symbol == null) { 
                    return LEFT_UNDEFINED;
                } else if (s2.symbol == null) { 
                    return RIGHT_UNDEFINED;
                } else { 
                    int diff = s1.symbol.compareTo(s2.symbol);
                    return diff < 0 ? LT : diff == 0 ? EQ : GT;
                }
            case 1:
                return s1.price < s2.price ? LT : s1.price == s2.price ? EQ : GT;
            case 2:
                return s1.volume < s2.volume ? LT : s1.volume == s2.volume ? EQ : GT;
            default:
                throw new IllegalArgumentException();
            }
        }
                
        public int getNumberOfDimensions() { 
            return 3;
        }
        
        public IPersistent cloneField(IPersistent obj, int i) { 
            Stock src = (Stock)obj;
            Stock clone = new Stock();
            switch (i) { 
            case 0:
                clone.symbol = src.symbol;
                break;
            case 1:
                clone.price = src.price;
                break;
             case 2:
                clone.volume = src.volume;
                break;
             default:
                throw new IllegalArgumentException();
            }
            return clone;
        }
    }
      
       
    final static int nRecords = 100000;
    static final int pagePoolSize = 16*1024*1024;
    static final int MAX_SYMBOLS = 1000;
    static final int MAX_PRICE = 100;
    static final int MAX_VOLUME = 10000;
    static final int EPSILON = 100;
    static final int KD_TREE_OPTIMIZATION_THRESHOLD = 3;
    
    static final int FIRST_STOCK_ID = 0xA0000;
    static int stockCount = FIRST_STOCK_ID;

    public static Stock getRandomStock(Random r) { 
        Stock s = new Stock();
        //s.symbol = Integer.toHexString(++stockCount);
        s.symbol = Integer.toHexString(r.nextInt(MAX_SYMBOLS));
        s.price = (float)r.nextInt(MAX_PRICE*10)/10;
        s.volume = r.nextInt(MAX_VOLUME);
        return s;
    }
    
    static public void main(String[] args) {    
        Storage db = StorageFactory.getInstance().createStorage();
        db.open("testkdtree2.dbs", pagePoolSize);
        boolean populate = args.length > 0 && args[0].equals("populate");

        MultidimensionalIndex index = (MultidimensionalIndex)db.getRoot();
        if (index == null) { 
            index = db.createMultidimensionalIndex(new StockComparator());
            db.setRoot(index);
        }
        Random r;
        long start;
        if (index.size() == 0) { 
            r = new Random(2007);
            start = System.currentTimeMillis();
            for (int i = 0; i < nRecords; i++) { 
                Stock s = getRandomStock(r);
                index.add(s);
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
        stockCount = FIRST_STOCK_ID;
        for (int i = 0; i < nRecords; i++) {
            Stock s = getRandomStock(r);            
            IPersistent[] result = index.queryByExample(s);
            Assert.that(result.length >= 1);
            for (int j = 0; j < result.length; j++) { 
                Assert.that(s.eq((Stock)result[j]));
            }
        }
        System.out.println("Elapsed time for performing " + nRecords + " query by example searches: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");
 
        start = System.currentTimeMillis();
        r = new Random(2007);
        Random r2 = new Random(2008);
        long total = 0;
        for (int i = 0; i < nRecords; i++) {
            Stock s = getRandomStock(r);            
            Stock min = new Stock();
            Stock max = new Stock();

            min.price = s.price - (float)MAX_PRICE/EPSILON;
            min.volume = s.volume - MAX_VOLUME/EPSILON;

            max.price = s.price + (float)MAX_PRICE/EPSILON;
            max.volume = s.volume + MAX_VOLUME/EPSILON;

            Iterator iterator = index.iterator(min, max);
            int n = 0;
            while (iterator.hasNext()) { 
                s = (Stock)iterator.next();
                Assert.that(min.le(s));
                Assert.that(s.le(max));
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
                Stock s = (Stock)iterator.next();
                iterator.remove();
                s.deallocate();
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
