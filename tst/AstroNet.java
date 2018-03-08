import java.io.*;
import org.garret.perst.*;

/**
 * This test loads stars catalog.
 * Data should be sent to the standard input of this program in CSV format: 
 *     RA,DEC 
 * where RA is right ascension and DEC declination of star in degrees.
 * This example was tested with USNO catalog containing about one milliard stars,
 * Perst was about 5 times faster than PostgreSQL with pgsphere module
 */
public class AstroNet
{
    public static class Star extends Persistent { 
        Sphere.Point point;

        double distance(Star star) { 
            return point.distance(star.point);
        }
    };

    static int pagePoolSize = 256*1024;
    public static void main(String[] args) throws Exception 
    { 
        DataInputStream in = new DataInputStream(new BufferedInputStream(System.in));
        String str;
        long i;
        long start = System.currentTimeMillis();
        Storage db = StorageFactory.getInstance().createStorage();
        db.open("usno.dbs", pagePoolSize);
        SpatialIndexRn<Star> index = db.< SpatialIndexRn<Star> >getRoot();
        if (index == null) { 
            index = db.<Star>createSpatialIndexRn();
            db.setRoot(index);
            for (i = 0; (str = in.readLine()) != null; i++) { 
                String[] vals = str.split(",");
                Star star = new Star();
                star.point = new Sphere.Point(Math.toRadians(Float.parseFloat(vals[0])), Math.toRadians(Float.parseFloat(vals[1])));
                index.put(star.point.wrappingRectangle(), star);
                if (i % 10000000 == 0) { 
                    System.out.print("Load " + i + " stars...\r");
                    db.commit();
                }
            }
            db.commit();
            System.out.println("Elapsed time for loading " + i + " stars: " + (System.currentTimeMillis() - start) + " msec");
        }
        start = System.currentTimeMillis();
        
        long totalMatches = 0;         
        long nSearches = 0;
        long nFalseMatches = 0;
        int maxMatches = 0;
        int minMatches = Integer.MAX_VALUE;
        start = System.currentTimeMillis();
        if (args.length == 2) { // locate starts in particular circle
            int ra = Integer.parseInt(args[0]);
            int dec = Integer.parseInt(args[1]);
            Sphere.Circle circle = new Sphere.Circle(new Sphere.Point(Math.toRadians(ra), Math.toRadians(dec)), Math.toRadians(1));
            int n = 0;
            for (Star star : index.iterator(circle.wrappingRectangle())) { 
                if (circle.contains(star.point)) {
                    System.out.println(star.point);
                    n += 1;
                } 
            }
            System.out.println("Locate " + n + " stars");
        } else if (args.length == 1) { // crossmatch
            double precision = Math.toRadians(Double.parseDouble(args[0])/3600); // procesion in arcseconds
            int n = 0;
            for (Star observedStar : index) {
                Sphere.Circle circle = new Sphere.Circle(observedStar.point, precision);
                double nearestDistance = Double.MAX_VALUE;
                Star nearestStar = null;
                for (Star catalogStar : index.iterator(circle.wrappingRectangle())) { 
                    double distance = catalogStar.distance(observedStar);
                    if (distance < nearestDistance) { 
                        nearestDistance = distance;
                        nearestStar = catalogStar;
                    }
                }
                if (nearestDistance <= precision) { 
                    n += 1;
                }
            }
            System.out.println("Elapsed time for " + n + " successful crossmatches with " + index.size() + " stars: " + (System.currentTimeMillis() - start) + " msec");
        } else { // perform cirtcle search of all sky with step 1'
            for (int ra = 0; ra < 360; ra++) { 
                for (int dec = -90; dec < 90; dec++) { 
                    Sphere.Circle circle = new Sphere.Circle(new Sphere.Point(Math.toRadians(ra), Math.toRadians(dec)), Math.toRadians(1));
                    int n = 0;
                    for (Star star : index.iterator(circle.wrappingRectangle())) { 
                        if (circle.contains(star.point)) {
                            n += 1;
                        } else { 
                            nFalseMatches += 1;
                        } 
                    }
                    totalMatches += n;
                    if (n > maxMatches) { 
                        maxMatches = n;
                    }
                    if (n < minMatches) { 
                        minMatches = n;
                    }
                    nSearches += 1;
                }
            }
            System.out.println("Elapsed time for " + nSearches + " 1' circle searches: " + (System.currentTimeMillis() - start) + " msec, " + totalMatches + " true matches, " + nFalseMatches + " false matches");
            System.out.println("Average number of stars in 1 degree circle: " + totalMatches/nSearches + ", minimum: " + minMatches + ", maximum: " + maxMatches);            
        }
        db.close();
    }
}