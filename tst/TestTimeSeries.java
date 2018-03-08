import org.garret.perst.*;

import java.util.*;

public class TestTimeSeries { 
    static final int MSECS_PER_DAY = 24*60*60*1000;
    
    public static class Quote implements TimeSeries.Tick { 
        int   date;
        float low;
        float high;
        float open;
        float close;
        int   volume;

        public long getTime() { 
            return (long)date*MSECS_PER_DAY;
        }
    }
    
    public static class QuoteBlock extends TimeSeries.Block {
        private Quote[] quotes;
        
        static final int N_ELEMS_PER_BLOCK = 100;

        public TimeSeries.Tick[] getTicks() { 
            if (quotes == null) { 
                quotes = new Quote[N_ELEMS_PER_BLOCK];
                for (int i = 0; i < N_ELEMS_PER_BLOCK; i++) { 
                    quotes[i] = new Quote();
                }
            }
            return quotes;
        }
    }

    static class Stock extends Persistent { 
        String            name;
        TimeSeries<Quote> quotes;
    }

    final static int nElements = 10*365;
    final static int pagePoolSize = 32*1024*1024;

    static final String[] months = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};

    static public void main(String[] args) throws Exception {   
        Stock stock;
        int i;

        Storage db = StorageFactory.getInstance().createStorage();
        db.open("testts.dbs", pagePoolSize);
        FieldIndex<Stock> stocks = db.<FieldIndex<Stock>>getRoot();
        if (stocks == null) { 
            stocks = db.<Stock>createFieldIndex(Stock.class, "name", true);
            stock = new Stock();
            stock.name = "BORL";
            stock.quotes = db.createTimeSeries(QuoteBlock.class, (long)QuoteBlock.N_ELEMS_PER_BLOCK*MSECS_PER_DAY*2);
            stocks.put(stock);
            db.setRoot(stocks);
        } else { 
            stock = (Stock)stocks.get("BORL");
        }
        Random rand = new Random(2004);
        long start = System.currentTimeMillis();
        int date = (int)(start/MSECS_PER_DAY) - nElements;
        for (i = 0; i < nElements; i++) { 
            Quote quote = new Quote();        
            quote.date = date + i;
            quote.open = (float)rand.nextInt(10000)/100;
            quote.close = (float)rand.nextInt(10000)/100;
            quote.high = Math.max(quote.open, quote.close);
            quote.low = Math.min(quote.open, quote.close);
            quote.volume = rand.nextInt(1000);
            stock.quotes.add(quote);
        }
        db.commit();
        System.out.println("Elapsed time for storing " + nElements + " quotes: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");
        
        rand.setSeed(2004);
        start = System.currentTimeMillis();
        Iterator<Quote> iterator = stock.quotes.iterator();
        for (i = 0; iterator.hasNext(); i++) { 
            Quote quote = iterator.next();
            Assert.that(quote.date == date + i);
            float open = (float)rand.nextInt(10000)/100;
            Assert.that(quote.open == open);
            float close = (float)rand.nextInt(10000)/100;
            Assert.that(quote.close == close);
            Assert.that(quote.high == Math.max(quote.open, quote.close));
            Assert.that(quote.low == Math.min(quote.open, quote.close));
            Assert.that(quote.volume == rand.nextInt(1000));
        }
        Assert.that(i == nElements);
        System.out.println("Elapsed time for extracting " + nElements + " quotes: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");
                 
        Assert.that(stock.quotes.size() == nElements);
        
        int shift = 1000;
        int count = 1000;
        long from = (long)(date+shift)*MSECS_PER_DAY;
        start = System.currentTimeMillis();
        iterator = stock.quotes.iterator(new Date(from), new Date(from + (long)count*MSECS_PER_DAY), false);
        for (i = 0; iterator.hasNext(); i++) { 
            Quote quote = iterator.next();
            Assert.that(quote.date == date + shift + count - i);
        }
        Assert.that(i == count+1);
        System.out.println("Elapsed time for extracting " + i + " quotes: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");

        start = System.currentTimeMillis();
        Map<Object,Aggregator.Aggregate> result = 
            Aggregator.<Quote>aggregate(stock.quotes.iterator(new Date(from), new Date(from + (long)count*MSECS_PER_DAY), false), 
                                        new Aggregator.GroupBy<Quote>() {
                                            public Aggregator.Aggregate getAggregate() { return new Aggregator.DevAggregate(); } 
                                            public Object getKey(Quote quote) { return (new Date(quote.getTime())).getMonth(); }
                                            public Object getValue(Quote quote) { return quote.high - quote.low; }
                                        }, true);
        for (Map.Entry<Object,Aggregator.Aggregate> pair : result.entrySet()) { 
            System.out.println(months[((Integer)pair.getKey()).intValue()] + ": " + pair.getValue().result());
        }

        start = System.currentTimeMillis();
        long n = stock.quotes.remove(stock.quotes.getFirstTime(), stock.quotes.getLastTime());
        Assert.that(n == nElements);
        System.out.println("Elapsed time for removing " + nElements + " quotes: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");

        Assert.that(stock.quotes.size() == 0);
        
        db.close();
    }
}




