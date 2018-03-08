import org.garret.perst.*;

public class BenchmarkMain 
{
    private final static int pagePoolSize = 32*1024*1024;

    private final static int fTest1Conn = 0;
    private final static int fTest3Conn = 1;
    private final static int fTiny = 2;
    private final static int fSmall = 3;
    private final static int fLarge = 4;
   
    static void printUsage() {
        System.out.println( "usage: OO7 (create|query) [options]" );
        System.out.println( "    create options:" );
        System.out.println( "        size        - (tiny|small|large)" );
        System.out.println( "    query options:" );
        System.out.println( "        type        - (traversal|match)" );
    } 
        
    public static void main( String[] args ) throws Exception {
        if (args.length == 0) {
            printUsage();
            System.exit( 1 );
        } else {
            if (args.length == 1 && args[1] == "query") {
                printUsage();
                System.exit( 1 );
            } 
        } 
        
        Storage db = StorageFactory.getInstance().createStorage();

        db.open("007.dbs", pagePoolSize);
            
        long start = System.currentTimeMillis();
        
        if (args[0].equals( "query" )) {
            if (args[1].equals( "traversal" )) {
                Benchmark anBenchmark = (Benchmark)db.getRoot();
                anBenchmark.traversalQuery();
            } else {
                if (args[1].equals( "match" )) {
                    Benchmark anBenchmark = (Benchmark)db.getRoot();
                    anBenchmark.matchQuery();
                } 
            } 
        } else {
            if (args[0].equals( "create" )) {
                int scale = -1;
                if (args[1].equals( "test3Conn" )) {
                    scale = fTest3Conn;
                } else if (args[1].equals( "test1Conn" )) {
                    scale = fTest1Conn;
                } else if (args[1].equals( "tiny" )) {
                    scale = fTiny;
                } else if (args[1].equals( "small" )) {
                    scale = fSmall;
                } else if (args[1].equals( "large" )) {
                    scale = fLarge;
                } else {
                    System.out.println( "Invalid scale" );
                    System.exit( 1 );
                } 
                Benchmark anBenchmark = new BenchmarkImpl();
                db.setRoot(anBenchmark);
                anBenchmark.create( scale );
            } 
        } 
            
        System.out.println( "time: " + (System.currentTimeMillis() - start) + "msec" );
        
        // close the connection
        db.close();
    } 
}
    
