import org.garret.perst.*;

import java.util.*;

class Restaurant extends Persistent
{                                               
    float lat;
    float lng; 
    String kitchen;
    int avgPrice;
    int rating;
}

class City extends Persistent
{
    SpatialIndexR2<Restaurant> byLocation;
    FieldIndex<Restaurant> byKitchen;
    FieldIndex<Restaurant> byAvgPrice;
    FieldIndex<Restaurant> byRating;
}

public class TestBitmap
{
    final static int nRecords = 1000000;
    final static int nSearches = 1000;
    static int pagePoolSize = 48*1024*1024;
    static String[] kitchens = {"asian", "chines", "european", "japan", "italian", "french", "medeteranian", "nepal", "mexican", "indian", "vegetarian"};
    public static void main(String[] args) { 
        Storage db = StorageFactory.getInstance().createStorage();
        db.open("testbitmap.dbs", pagePoolSize);

        City city = (City)db.getRoot();
        if (city == null) { 
            city = new City();
            city.byLocation = db.<Restaurant>createSpatialIndexR2();
            city.byKitchen = db.<Restaurant>createFieldIndex(Restaurant.class, "kitchen", false, true, true);
            city.byAvgPrice = db.<Restaurant>createFieldIndex(Restaurant.class, "avgPrice", false, true, true);
            city.byRating = db.<Restaurant>createFieldIndex(Restaurant.class, "rating", false, true, true);
            db.setRoot(city);
        }
        Random rnd = new Random(2013);
        long start = System.currentTimeMillis();

        for (int i = 0; i < nRecords; i++) { 
            Restaurant rest = new Restaurant();
            rest.lat = 55 + rnd.nextFloat();
            rest.lng = 37 + rnd.nextFloat();
            rest.kitchen = kitchens[rnd.nextInt(kitchens.length)];
            rest.avgPrice = rnd.nextInt(1000);
            rest.rating = rnd.nextInt(10);
            city.byLocation.put(new RectangleR2(rest.lat, rest.lng, rest.lat, rest.lng), rest);
            city.byKitchen.put(rest);
            city.byAvgPrice.put(rest);
            city.byRating.put(rest);
        }
        db.commit();
        System.out.println("Elapsed time for inserting " + nRecords + " records: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");


        start = System.currentTimeMillis();
        long total = 0;                         
        long nProbes = 0;
        for (int i = 0; i < nSearches; i++) {
            int oid;
            double lat = 55 + rnd.nextFloat();
            double lng = 37 + rnd.nextFloat();
            String kitchen = kitchens[rnd.nextInt(kitchens.length)];
            int minPrice = rnd.nextInt(1000);
            int maxPrice = minPrice + rnd.nextInt(1000);
            int minRating = rnd.nextInt(10);
            Bitmap bitmap = db.createBitmap(city.byKitchen.iterator(kitchen, kitchen, Index.ASCENT_ORDER));
            bitmap.and(db.createBitmap(city.byAvgPrice.iterator(minPrice, maxPrice, Index.ASCENT_ORDER)));
            bitmap.and(db.createBitmap(city.byRating.iterator(minRating, null, Index.ASCENT_ORDER)));
            PersistentIterator iterator = (PersistentIterator)city.byLocation.neighborIterator(lat, lng);

            int nAlternatives = 0;             
            while ((oid = iterator.nextOid()) != 0) { 
                nProbes += 1;
                if (bitmap.contains(oid)) { 
                    Restaurant rest = (Restaurant)db.getObjectByOID(oid);
                    total += 1;
                    if (++nAlternatives == 10) { 
                        break;
                    }
                }
            }
        }
        System.out.println("Elapsed time for " + nSearches + " searches " + nProbes + " probes of " + total + " variants among " + nRecords + " records: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");
        db.close();
    }
}
