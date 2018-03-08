package org.garret.perst;

import java.util.*;

/**
 * Class used to perform aggregation.
 * Grouping and aggregation is done using two interfaces <code>Aggregate</code> and <code>GroupBy</code>.
 * <code>Aggregate</code> interface is used to provide aggregate implementations. 
 * There are some predefined implementations for standard aggregates: max, min, sum, avg, count... 
 * And it is possible to define own aggregates.<p>
 * <code>GroupBy</code> interface should be implemented by programmer (it is convenient to use anonymous classes)
 * and defines aggregation query: how to split input data into groups and which aggregates to calculate for the group.
 * So this class gives answer for the questions:
 * <ol>
 * <li>Which field(s) are used for grouping (group-by fields)?</li>
 * <li>For which field aggregate(s) should be calculated?</li>
 * <li>Which aggregate(s) should be calculated for each group?</li>
 * </ol>
 * Aggregator is using map to associate aggregate states with groups. This map is returned as result of 
 * aggregation. Aggregator can use ordered or unordered map (TreeMap or HashMap). Ordered map returns results in ascending 
 * order of group-by values.
 * <p>
 * Example:
 * <pre>
 *     class Quote implements TimeSeries.Tick { 
 *         public long  date;
 *         public float open;
 *         public float close;
 *         public float low;
 *         public float high;
 *         public int   volume;
 * 
 *         public long getTime() { 
 *            return date;
 *         }
 *     };
 *     // Query: select  standard deviation of difference between low and high prices for IBM for each month during 1990 till 2012
 *     Stock stock = stocks.get("IBM");
 *     IterableIterator&lt;Quote&gt; iterator = stock.quotes.iterator(new Date(1990, 0, 1), new Date(2012, 0, 1));
 *     Map&lt;Object,Aggregator.Aggregate&gt; result = Aggregator.&lt;Quote&gt;aggregate(iterator, new Aggregator.GroupBy&lt;Quote&gt;() {
 *         public Aggregator.Aggregate getAggregate() { return new Aggregator.DevAggregate(); } 
 *         public Object getKey(Quote quote) { return (new Date(quote.date)).getMonth(); }
 *         public Object getValue(Quote quote) { return quote.high - quote.low; }
 *     }, true);
 *     for (Map.Entry&lt;Object,Aggregator.Aggregate&gt; pair : result) { 
 *         System.out.println("Group " + pair.getKey() + "-&gt;" + pair.getValue().result());
 *     }
 * </pre>
 */
public class Aggregator
{
    /**
     * Aggregate interface. It is implementye by all standard aggregates and can be use to define own (custom) aggregates
     */
    public interface Aggregate<T>
    { 
        /**
         * Get aggregate value for this group
         * @return aggregate value
         */
        Object result();
        /**
         * Initialize aggregate state 
         * @param val first aggregated value for this group
         */
        void initialize(T val);
        /**
         * Accumulate aggregate state 
         * @param val subsequent aggregated value for this group
         */
        void accumulate(T val);
        /**
         * Merge two aggregate states. This method is used by Aggregator.merge method by also can be use explicitly to combine two aggregation results.
         * @param other another iterator of the same type
         */
        void merge(Aggregate<T> other);
    }

    /**
     * Interface used to specify aggregation operation
     */
    public interface GroupBy<T> 
    {
        /**
         * Used aggregate 
         */
        Aggregate getAggregate();

        /**
         * Group-by field(s). 
         * @return For single field it is ebough to return wrapper object for the field value.
         * For multiple fields you should construct class and implement Object.hashCode(), Object.equals() and optionally Comparable.compareTo() methods
         */
        Object getKey(T obj);

        /** 
         * Aggregated field. 
         * @return Wrapper class for aggregated field value.
         */
        Object getValue(T obj);
    }

    /**
     * Perform aggregation
     * @param iterable collection of aggregated objects
     * @param groupBy aggregation operation
     * @return map with results of aggregation: &lt;group-by,aggregate-value&gt; pairs
     */
    public static <T> Map<Object,Aggregate> aggregate(Iterable<T> iterable, GroupBy<T> groupBy) 
    {
        return aggregate(iterable, groupBy, false);
    }
 
    /**
     * Perform aggregation
     * @param iterable collection of aggregated objects
     * @param groupBy aggregation operation
     * @param orderByKey specify whether ordered map (TreeMap) should be used for grouping. Group-by kley should privide comparison operation in this case. 
     * @return map with results of aggregation: &lt;group-by,aggregate-value&gt; pairs
     */
    public static <T> Map<Object,Aggregate> aggregate(Iterable<T> iterable, GroupBy<T> groupBy, boolean orderByKey) 
    { 
        Iterator<T> iterator = iterable.iterator();
        Map<Object,Aggregate> hash = orderByKey 
           ? (Map<Object,Aggregate>)new TreeMap<Object,Aggregate>() 
           : (Map<Object,Aggregate>)new HashMap<Object,Aggregate>(); 
        while (iterator.hasNext()) { 
            T obj = iterator.next();
            Object key = groupBy.getKey(obj);
            Object value = groupBy.getValue(obj);
            Aggregate agg = hash.get(key);
            if (agg == null) { 
                agg = groupBy.getAggregate();
                hash.put(key, agg);
                agg.initialize(value);
            } else { 
                agg.accumulate(value);
            }
        }
        return hash;
    }

    /**
     * Merge two aggregation results. This method combines state of aggregates in "dst" with aggregate states in "src", i.e. dst = merge(dst, src)
     */
    public static void merge(Map<Object,Aggregate> dst, Map<Object,Aggregate> src) 
    {
        for (Map.Entry<Object,Aggregate> pair : src.entrySet()) {
            Object key = pair.getKey();
            Aggregate srcAgg = pair.getValue();
            Aggregate dstAgg = dst.get(key);
            if (dstAgg == null) {
                dst.put(key, srcAgg);
            } else { 
                dstAgg.merge(srcAgg);
            }
        }
    }
                            
    /**
     * Aggregate returning top N values 
     */
    public static class TopAggregate implements Aggregate<Comparable>
    {
        public void initialize(Comparable val) {
            max[0] = val;
            used = 1;
        }
    
        public void accumulate(Comparable val) { 
            int l = 0, n = used, r = n;
            while (l < r) { 
                int m = (l + r) >>> 1;
                if (val.compareTo(max[m]) < 0)  { 
                    l = m + 1;
                } else {
                    r = m;
                }
            }
            if (used < max.length) { 
                System.arraycopy(max, r, max, r+1, used-r);
                max[r] = val;
                used += 1;
            } else if (r < used) { 
                System.arraycopy(max, r, max, r+1, used-r-1);
                max[r] = val;
            }
        }

        /**
         * Result of aggregation
         * @return array with top N values. If there are less than N object in aggregated set, then size of result array may be smaller than N 
         */
        public Object result() { 
            if (used < max.length) { 
                Comparable[] res = new Comparable[used];
                System.arraycopy(max, 0, res, 0, used);
                return res;
            }
            return max;
        }

        public void merge(Aggregate<Comparable> other) { 
            for (Comparable obj : (Comparable[])other.result()) { 
                accumulate(obj);
            }
        }

        /**
         * Aggregate constructor
         * @param n top N
         */
        public TopAggregate(int n) { 
            max = new Comparable[n];
            used = 0;
        }

        Comparable[] max;
        int used;
    }

    /**
     * Maximum aggregate 
     */
    public static class MaxAggregate implements Aggregate<Comparable>
    {
        public void initialize(Comparable val) {
            max = val;
        }
    
        public void accumulate(Comparable val) { 
            if (val.compareTo(max) > 0) { 
                max = val;
            }
        }

        public Object result() { 
            return max;
        }
        
        public void merge(Aggregate<Comparable> other) { 
            accumulate((Comparable)other.result());
        }

        Comparable max;
    }

    /**
     * Minimum aggregate
     */
    public static class MinAggregate implements Aggregate<Comparable>
    {
        public void initialize(Comparable val) {
            min = val;
        }
    
        public void accumulate(Comparable val) { 
            if (val.compareTo(min) < 0) { 
                min = val;
            }
        }

        public Object result() { 
            return min;
        }

        public void merge(Aggregate<Comparable> other) { 
            accumulate((Comparable)other.result());
        }

        Comparable min;
    }

    /**
     * Sum aggregate for real values
     */
    public static class RealSumAggregate implements Aggregate<Number>
    {
        public void initialize(Number val) {
            sum = val.doubleValue();
        }
    
        public void accumulate(Number val) { 
            sum += val.doubleValue();
        }

        public Object result() { 
            return new Double(sum);
        }

        public void merge(Aggregate<Number> other) { 
            sum += ((RealSumAggregate)other).sum;
        }

        double sum;
    }

    /**
     * Sum aggregate for integer values
     */
    public static class IntegerSumAggregate implements Aggregate<Number>
    {
        public void initialize(Number val) {
            sum = val.longValue();
        }
    
        public void accumulate(Number val) { 
            sum += val.longValue();
        }

        public Object result() { 
            return new Long(sum);
        }

        public void merge(Aggregate<Number> other) { 
            sum += ((IntegerSumAggregate)other).sum;
        }

        long sum;
    }

    /**
     * Average aggregate
     */
    public static class AvgAggregate implements Aggregate<Number>
    {
        public void initialize(Number val) {
            sum = val.doubleValue();
            count = 1;
        }
    
        public void accumulate(Number val) { 
            sum += val.doubleValue();
            count += 1;
        }

        public Object result() { 
            return new Double(sum/count);
        }

        public void merge(Aggregate<Number> other) { 
            AvgAggregate otherAvg = (AvgAggregate)other;
            sum += otherAvg.sum;
            count += otherAvg.count;
        }

        double sum;
        long count;
    }

    /**
     * Product aggregate
     */
    public static class PrdAggregate implements Aggregate<Number>
    {
        public void initialize(Number val) {
            prd = val.doubleValue();
        }
    
        public void accumulate(Number val) { 
            prd *= val.doubleValue();
        }

        public Object result() { 
            return new Double(prd);
        }

        public void merge(Aggregate<Number> other) { 
            prd *= ((PrdAggregate)other).prd;
        }

        double prd;
    }

    /**
     * Variance aggregate
     */
    public static class VarAggregate implements Aggregate<Number>
    {
        public void initialize(Number val) {
            double v = val.doubleValue();
            sum = v;
            sum2 = v*v;
            count = 1;
        }
    
        public void accumulate(Number val) { 
            double v = val.doubleValue();
            sum += v;
            sum2 += v*v;
            count += 1;
        }

        public Object result() { 
            return new Double((sum2 - sum*sum/count)/count);
        }

        public void merge(Aggregate<Number> other) { 
            VarAggregate otherVar = (VarAggregate)other;
            sum += otherVar.sum;
            sum2 += otherVar.sum2;
            count += otherVar.count;
        }

        double sum;
        double sum2;
        long count;
    }

    /**
     * Standard deviation aggregate
     */
    public static class DevAggregate extends VarAggregate
    {
        public Object result() { 
            return new Double(Math.sqrt((sum2 - sum*sum/count)/count));
        }
    }
    
    /**
     * Count all aggregate
     */
    public static class CountAggregate implements Aggregate
    {
        public void initialize(Object val) {
            count = 1;
        }
    
        public void accumulate(Object val) { 
            count += 1;
        }

        public Object result() { 
            return new Long(count);
        }

        public void merge(Aggregate other) { 
            count += ((CountAggregate)other).count;
        }

        long count;
    }
        
    /**
     * Distinct count aggregate (large memory footprint)
     */
    public static class DistinctCountAggregate implements Aggregate
    {
        public void initialize(Object val) {
            accumulate(val);
        }
    
        public void accumulate(Object val) { 
            set.add(val);
        }

        public Object result() { 
            return new Long(set.size());
        }

        public void merge(Aggregate other) { 
            set.addAll(((DistinctCountAggregate)other).set);
        }

        Set set = new HashSet();
    }
        
    /**
     * Count number of items repeated N or more times 
     */
    public static class RepeatCountAggregate implements Aggregate
    {
        static class Counter { 
            int value;

            Counter(int val) { 
                value = val;
            }
        }

        public void initialize(Object val) {
            accumulate(val);
        }
    
        public void accumulate(Object val) { 
            Counter counter = occ.get(val);
            if (counter == null) { 
                counter = new Counter(1);
                occ.put(val, counter);
            } else { 
                counter.value += 1;
            }
            if (counter.value == minOccurrences) { 
                count += 1;
            }
        }

        public Object result() { 
            return new Long(count);
        }

        public void merge(Aggregate other) { 
            for (Map.Entry<Object,Counter> pair : ((RepeatCountAggregate)other).occ.entrySet()) { 
                Object key = pair.getKey();
                Counter counter = occ.get(key);
                if (counter == null) { 
                    counter = pair.getValue();
                    occ.put(key, counter);
                    if (counter.value >= minOccurrences) { 
                        count += 1;
                    }
                } else {
                    int oldValue = counter.value;
                    counter.value += pair.getValue().value;
                    if (oldValue < minOccurrences && counter.value >= minOccurrences) { 
                        count += 1;
                    }
                }
            }
        }

        /**
         * Constructor of aggregate
         * @param minOccurrences minimal number of occurrences
         */
        public RepeatCountAggregate(int minOccurrences) { 
            this.minOccurrences = minOccurrences;
        }

        HashMap<Object,Counter> occ = new HashMap<Object,Counter>();
        int minOccurrences;
        long count;
    }
        
    /**
     * Approximate distinct count aggregate (not precise result)
     */
    public static class ApproxDistinctCountAggregate implements Aggregate
    {
        static final int HASH_BITS = 25;
        static final int N_HASHES = 1 << (32 - HASH_BITS);

        public void initialize(Object val) {
            accumulate(val);
        }
    
        public void accumulate(Object val) { 
            int h = val.hashCode();
            int j = h >>> HASH_BITS;
            int zeroBits = 1;
            while ((h & 1) == 0 && zeroBits <= HASH_BITS)  {
                h >>>= 1;
                zeroBits += 1;
            }
            if (maxZeroBits[j] < zeroBits) { 
                maxZeroBits[j] = zeroBits;
            }            
        }

        public Object result()
        { 
            final int m = N_HASHES;
            final double alpha_m = 0.7213 / (1 + 1.079 / (double)m);
            final double pow_2_32 = 0xffffffff;
            double E, c = 0;
            int i;
            for (i = 0; i < m; i++)
            {
                c += 1 / Math.pow(2., (double)maxZeroBits[i]);
            }
            E = alpha_m * m * m / c;    
            
            if (E <= (5 / 2. * m))
            {
                double V = 0;
                for (i = 0; i < m; i++)
                {
                    if (maxZeroBits[i] == 0) { 
                        V += 1;
                    }
                }                
                if (V > 0)
                {
                    E = m * Math.log(m / V);
                }
            }
            else if (E > (1 / 30. * pow_2_32))
            {
                E = -pow_2_32 * Math.log(1 - E / pow_2_32);
            }
            return new Long((long)E);
        }
        
        public void merge(Aggregate other) { 
            int[] otherMaxZeroBits = ((ApproxDistinctCountAggregate)other).maxZeroBits;
            for (int i = 0; i < N_HASHES; i++) { 
                if (maxZeroBits[i] < otherMaxZeroBits[i]) { 
                    maxZeroBits[i] = otherMaxZeroBits[i];
                }
            }
        }

        int[] maxZeroBits = new int[N_HASHES];
    } 
       
    /**
     * First group element aggregate 
     */
    public static class FirstAggregate implements Aggregate
    {
        public void initialize(Object val) {
            first = val;
        }
    
        public void accumulate(Object val) { 
        }

        public Object result() { 
            return first;
        }

        public void merge(Aggregate other) {}
 
        Object first;
    }
       
    /**
     * Last group element aggregate 
     */
    public static class LastAggregate implements Aggregate
    {
        public void initialize(Object val) {
            last = val;
        }
    
        public void accumulate(Object val) { 
            last = val;
        }

        public Object result() { 
            return last;
        }

        public void merge(Aggregate other) { 
            last = ((LastAggregate)other).last;
        }

        Object last;
    }

    /**
     * Compound aggregate: combination of several aggregates allowsing to caclulate more than one aggregate at one traversal
     */
    public static class CompoundAggregate implements Aggregate
    {
        public void initialize(Object val) {
            for (Aggregate agg : aggregates) { 
                agg.initialize(val);
            }
        }
    
        public void accumulate(Object val) { 
            for (Aggregate agg : aggregates) { 
                agg.accumulate(val);
            }
        }

        public Object result() { 
            Object[] arr = new Object[aggregates.length];
            for (int i = 0; i < aggregates.length; i++) { 
                arr[i] = aggregates[i].result();
            }
            return arr;
        }
 
        public void merge(Aggregate other) { 
            Aggregate[] otherAggregates = ((CompoundAggregate)other).aggregates;
            for (int i = 0; i < otherAggregates.length; i++) { 
                aggregates[i].merge(otherAggregates[i]);
            }
        }

        public CompoundAggregate(Aggregate... aggs) { 
            aggregates = aggs; 
        }

        Aggregate[] aggregates;
    }
}
