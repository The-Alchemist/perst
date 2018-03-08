import org.garret.perst.*;

import java.util.*;
import java.text.*;
import java.net.*;
import java.io.*;
import java.math.BigInteger;

public class TestAgg 
{ 
    final static int PAGE_POOL_SIZE = 64*1024*1024;
    final static long MILLIS_PER_DAY = 24*3600*1000;
    final static long MB = 1024*1024;

    final static DateFormat dateFormatter = new SimpleDateFormat("yyyy-M-d");

    static class Int128 implements IValue
    { 
        long low;
        long high;

        public Int128() {}

        public Int128(long high, long low) { 
            this.high = high;
            this.low = low;
        }

        public boolean equals(Object o) { 
            return o instanceof Int128  && ((Int128)o).low == low && ((Int128)o).high == high;
        }

        public int hashCode() { 
            return (int)low ^ (int)(low >>> 32) ^ (int)high ^ (int)(high >>> 32);
        }

        public String toString() { 
            String lowStr = "000000000000000" +  Long.toHexString(low);
            return Long.toHexString(high) + lowStr.substring(lowStr.length() - 16);
        }
    }

    static class HostDay implements Comparable<HostDay>
    {
        String host;
        int day;

        HostDay(String host, int day) { 
            this.host = host;
            this.day = day;
        }

        public boolean equals(Object o) { 
            return o instanceof HostDay && ((HostDay)o).host.equals(host) && ((HostDay)o).day == day;
        }

        public int hashCode() { 
            return host.hashCode() ^ day;
        }

        public int compareTo(HostDay other) { 
            if (day != other.day) { 
                return day - other.day;
            }
            return host.compareTo(other.host);
        }

        public String toString() { 
            return host + "[" +  dateFormatter.format(new Date(day*MILLIS_PER_DAY)) + "]";
        }
    }
        
    public static class Event implements TimeSeries.Tick 
    { 
        int   day;
        Int128 user;
        int    ip;
        String host;
        int    url;
        String agent;

        Event() {
            user = new Int128();
        }

        public long getTime() { 
            return day*MILLIS_PER_DAY;
        }
    }
    
    public static class EventBlock extends TimeSeries.Block {
        private Event[] events;
        
        static final int N_ELEMS_PER_BLOCK = 1000;

        public TimeSeries.Tick[] getTicks() { 
            if (events == null) { 
                events = new Event[N_ELEMS_PER_BLOCK];
                for (int i = 0; i < N_ELEMS_PER_BLOCK; i++) { 
                    events[i] = new Event();
                }
            }
            return events;
        }
    }

    static class Job extends Thread { 
        TimeSeries<Event> events;
        Date date;
        Aggregator.GroupBy<Event> groupBy;
        boolean sort;
        Map<Object,Aggregator.Aggregate> result;

        Job(TimeSeries<Event> events, Date date, Aggregator.GroupBy<Event> groupBy, boolean sort) { 
            this.events = events;
            this.date = date;
            this.groupBy = groupBy;
            this.sort = sort;
        }

        public void run() { 
            result = Aggregator.<Event>aggregate(events.iterator(date, date), groupBy, sort);
        }
    }

    static void report(TimeSeries<Event> events, String name, Aggregator.GroupBy<Event> groupBy, String resultFile, boolean sort, boolean parallel) throws Exception
    { 
        long start = System.currentTimeMillis();
        Map<Object,Aggregator.Aggregate> result;
        if (parallel) { 
            long first = events.getFirstTime().getTime();
            long last = events.getLastTime().getTime();
            int nJobs = (int)((last - first)/MILLIS_PER_DAY) + 1;
            Job[] jobs = new Job[nJobs];
            System.out.println("Start " + nJobs + " jobs...");
            for (int i = 0; i < nJobs; i++) { 
                jobs[i] = new Job(events, new Date(first + i*MILLIS_PER_DAY), groupBy, sort);
                jobs[i].start();
            }
            for (int i = 0; i < nJobs; i++) { 
                jobs[i].join();
            }
            result = jobs[0].result;
            for (int i = 1; i < nJobs; i++) { 
                Aggregator.merge(result, jobs[i].result);
            }            
        } else { 
            result = Aggregator.<Event>aggregate(events, groupBy, sort);
        }
        PrintStream out = new PrintStream(new FileOutputStream(resultFile));
        for (Map.Entry<Object,Aggregator.Aggregate> pair : result.entrySet()) { 
            out.println(pair.getKey() + "->" + pair.getValue().result());
        }
        out.close();
        System.out.println(name + ": " + (System.currentTimeMillis() - start) + " milliseconds");
    }

    static void load(TimeSeries<Event> events) throws Exception
    {
        long start = System.currentTimeMillis();
        BufferedReader in = new BufferedReader(new FileReader("data.csv"));
        String line;
        while ((line = in.readLine()) != null) { 
            if (!line.startsWith("set")) { 
                String[] cols = line.split(" ! ");
                if (cols.length != 7) { 
                    continue;
                }
                Assert.that(cols.length == 7);
                Event event = new Event();
                Date date = dateFormatter.parse(cols[0]);
                String user = cols[1];
                if (user.length() != 0) { 
                    if (user.length() > 16) { 
                        event.user.high = new BigInteger(user.substring(0, user.length()-16), 16).longValue();
                        event.user.low = new BigInteger(user.substring(user.length()-16), 16).longValue();
                    } else { 
                        event.user.low = new BigInteger(user, 16).longValue();
                    }
                }
                InetAddress ip = InetAddress.getByName(cols[2]);
                String host = cols[3];
                String url = cols[4];
                String agent = cols[6];
                
                event.day = (int)(date.getTime() / MILLIS_PER_DAY);
                event.ip = ip.hashCode();
                event.host = host;
                event.url = url.hashCode();
                event.agent = agent;

                events.add(event);
            }
        }
        System.out.println("Elapsed time for loading " + events.size() + " events: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");
    }
        

    static int parseOption(String option, int defaultValue) { 
        int valPos = option.indexOf('=');
        return valPos >= 0 ? Integer.parseInt(option.substring(valPos+1)) : defaultValue;
    }


    static public void main(String[] args) throws Exception 
    {   
        Storage db = StorageFactory.getInstance().createStorage();
        boolean inMem = false;
        long poolSize = PAGE_POOL_SIZE;
        int cache = 0;
        boolean sort = false;
        boolean par = false;
        for (int i = 0; i < args.length; i++) { 
            String option = args[i];
            if (option.startsWith("inmem")) { 
                inMem = parseOption(option, 1) != 0;
            } else if (option.startsWith("pool")) { 
                poolSize = parseOption(option, 0)*MB;
            } else if (option.startsWith("cache")) { 
                cache = parseOption(option, 0);
            } else if (option.startsWith("sort")) { 
                sort = parseOption(option, 1) != 0;
            } else if (option.startsWith("par")) { 
                par = parseOption(option, 1) != 0;
            } else { 
                System.err.println("Options:");
                System.err.println("\tinmem: main memory database (do not save data to the disk)");
                System.err.println("\tpool=size: size (Mb) of page pool, 0 for infinite pool");
                System.err.println("\tcache=size: number  of objects pinned in object cache, 0 - default serrings");
                System.err.println("\tsort: output results sorted by group-by key");
                System.err.println("\tpar: parallel evaluation of aggregates");
                return;
            }
        }
        db.setProperty("perst.string.encoding", "UTF8");        
        if (cache != 0) { 
            db.setProperty("perst.object.cache.init.size", Integer.toString(cache));
        }
        db.setProperty("perst.alternative.btree", "true");
        if (inMem) { 
            db.open(new NullFile(), Storage.INFINITE_PAGE_POOL);
        } else {
            db.open("testagg.dbs", poolSize);
        }
        TimeSeries<Event> events = db.<TimeSeries<Event>>getRoot();
        if (events == null) { 
            events = db.<Event>createTimeSeries(EventBlock.class, 1);
            db.setRoot(events);
            load(events);
            if (!inMem) { 
                long start = System.currentTimeMillis();
                db.commit();
                System.out.println("Commit time: " + (System.currentTimeMillis() - start) + " milliseconds");
            }
        }
        
        // Events per host

        report(events, "Events per host", new Aggregator.GroupBy<Event>() {
                public Aggregator.Aggregate getAggregate() { return new Aggregator.CountAggregate(); } 
                public Object getKey(Event event) { return event.host; }
                public Object getValue(Event event) { return null; }
            }, "events_per_host.res", sort, par);

        // IPs per host

        report(events, "Approximation of unique IPs per host", new Aggregator.GroupBy<Event>() {
                public Aggregator.Aggregate getAggregate() { return new Aggregator.ApproxDistinctCountAggregate(); } 
                public Object getKey(Event event) { return event.host; }
                public Object getValue(Event event) { return event.ip; }
            }, "uniq_ips_per_host.app", sort, par);

        report(events, "Unique IPs per host", new Aggregator.GroupBy<Event>() {
                public Aggregator.Aggregate getAggregate() { return new Aggregator.DistinctCountAggregate(); } 
                public Object getKey(Event event) { return event.host; }
                public Object getValue(Event event) { return event.ip; }
            }, "uniq_ips_per_host.res", sort, par);

        // URLs per host

        report(events, "Approximation of unique requests per host", new Aggregator.GroupBy<Event>() {
                public Aggregator.Aggregate getAggregate() { return new Aggregator.ApproxDistinctCountAggregate(); } 
                public Object getKey(Event event) { return event.host; }
                public Object getValue(Event event) { return event.url; }
            }, "uniq_reqs_per_host.app", sort, par);

        report(events, "Unique requests per host", new Aggregator.GroupBy<Event>() {
                public Aggregator.Aggregate getAggregate() { return new Aggregator.DistinctCountAggregate(); } 
                public Object getKey(Event event) { return event.host; }
                public Object getValue(Event event) { return event.url; }
            }, "uniq_reqs_per_host.res", sort, par);

        // Agents per host

        report(events, "Approximation of unique agents per host", new Aggregator.GroupBy<Event>() {
                public Aggregator.Aggregate getAggregate() { return new Aggregator.ApproxDistinctCountAggregate(); } 
                public Object getKey(Event event) { return event.host; }
                public Object getValue(Event event) { return event.agent; }
            }, "uniq_agents_per_host.app", sort, par);

        report(events, "Unique agents per host", new Aggregator.GroupBy<Event>() {
                public Aggregator.Aggregate getAggregate() { return new Aggregator.DistinctCountAggregate(); } 
                public Object getKey(Event event) { return event.host; }
                public Object getValue(Event event) { return event.agent; }
            }, "uniq_agents_per_host.res", sort, par);

        // Users per host

        report(events, "Approximation of unique users per host", new Aggregator.GroupBy<Event>() {
                public Aggregator.Aggregate getAggregate() { return new Aggregator.ApproxDistinctCountAggregate(); } 
                public Object getKey(Event event) { return event.host; }
                public Object getValue(Event event) { return event.user; }
            }, "uniq_users_per_host.app", sort, par);

        report(events, "Unique users per host", new Aggregator.GroupBy<Event>() {
                public Aggregator.Aggregate getAggregate() { return new Aggregator.DistinctCountAggregate(); } 
                public Object getKey(Event event) { return event.host; }
                public Object getValue(Event event) { return event.user; }
            }, "uniq_users_per_host.res", sort, par);

        // Frequenters per host

        report(events, "Frequenters per host", new Aggregator.GroupBy<Event>() {
                public Aggregator.Aggregate getAggregate() { return new Aggregator.RepeatCountAggregate(2); } 
                public Object getKey(Event event) { return event.host; }
                public Object getValue(Event event) { return event.user; }
            }, "frequenters_per_host.res", sort, par);


        // Events per host/day

        report(events, "Events per host/day", new Aggregator.GroupBy<Event>() {
                public Aggregator.Aggregate getAggregate() { return new Aggregator.CountAggregate(); } 
                public Object getKey(Event event) { return new HostDay(event.host, event.day); }
                public Object getValue(Event event) { return null; }
            }, "events_per_host_day.res", sort, par);

        // IPs per host/day

        report(events, "Approximation of unique IPs per host/day", new Aggregator.GroupBy<Event>() {
                public Aggregator.Aggregate getAggregate() { return new Aggregator.ApproxDistinctCountAggregate(); } 
                public Object getKey(Event event) { return new HostDay(event.host, event.day); }
                public Object getValue(Event event) { return event.ip; }
            }, "uniq_ips_per_host_day.app", sort, par);

        report(events, "Unique IPs per host/day", new Aggregator.GroupBy<Event>() {
                public Aggregator.Aggregate getAggregate() { return new Aggregator.DistinctCountAggregate(); } 
                public Object getKey(Event event) { return new HostDay(event.host, event.day); }
                public Object getValue(Event event) { return event.ip; }
            }, "uniq_ips_per_host_day.res", sort, par);

        // URLs per host/day

        report(events, "Approximation of unique requests per host/day", new Aggregator.GroupBy<Event>() {
                public Aggregator.Aggregate getAggregate() { return new Aggregator.ApproxDistinctCountAggregate(); } 
                public Object getKey(Event event) { return new HostDay(event.host, event.day); }
                public Object getValue(Event event) { return event.url; }
            }, "uniq_reqs_per_host_day.app", sort, par);

        report(events, "Unique requests per host/day", new Aggregator.GroupBy<Event>() {
                public Aggregator.Aggregate getAggregate() { return new Aggregator.DistinctCountAggregate(); } 
                public Object getKey(Event event) { return new HostDay(event.host, event.day); }
                public Object getValue(Event event) { return event.url; }
            }, "uniq_reqs_per_host_day.res", sort, par);

        // Agents per host/day

        report(events, "Approximation of unique agents per host/day", new Aggregator.GroupBy<Event>() {
                public Aggregator.Aggregate getAggregate() { return new Aggregator.ApproxDistinctCountAggregate(); } 
                public Object getKey(Event event) { return new HostDay(event.host, event.day); }
                public Object getValue(Event event) { return event.agent; }
            }, "uniq_agents_per_host_day.app", sort, par);

        report(events, "Unique agents per host/day", new Aggregator.GroupBy<Event>() {
                public Aggregator.Aggregate getAggregate() { return new Aggregator.DistinctCountAggregate(); } 
                public Object getKey(Event event) { return new HostDay(event.host, event.day); }
                public Object getValue(Event event) { return event.agent; }
            }, "uniq_agents_per_host_day.res", sort, par);

        // Users per host/day

        report(events, "Approximation of unique users per host/day", new Aggregator.GroupBy<Event>() {
                public Aggregator.Aggregate getAggregate() { return new Aggregator.ApproxDistinctCountAggregate(); } 
                public Object getKey(Event event) { return new HostDay(event.host, event.day); }
                public Object getValue(Event event) { return event.user; }
            }, "uniq_users_per_host_day.app", sort, par);

        report(events, "Unique users per host/day", new Aggregator.GroupBy<Event>() {
                public Aggregator.Aggregate getAggregate() { return new Aggregator.DistinctCountAggregate(); } 
                public Object getKey(Event event) { return new HostDay(event.host, event.day); }
                public Object getValue(Event event) { return event.user; }
            }, "uniq_users_per_host_day.res", sort, par);

        // Frequenters per host/day

        report(events, "Frequenters per host/day", new Aggregator.GroupBy<Event>() {
                public Aggregator.Aggregate getAggregate() { return new Aggregator.RepeatCountAggregate(2); } 
                public Object getKey(Event event) { return new HostDay(event.host, event.day); }
                public Object getValue(Event event) { return event.user; }
            }, "frequenters_per_host_day.res", sort, par);

        if (!inMem) { 
            db.close();
        }
    }
}




