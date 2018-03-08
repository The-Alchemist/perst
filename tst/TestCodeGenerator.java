import org.garret.perst.*;

import java.util.Date;
import java.util.ArrayList;

public class TestCodeGenerator { 
    static class Track { 
        int no;
    
        @Indexable
        Album album;
    
        @Indexable    
        String name;
    
        float duration;
    }
    
    static class Album { 
        @Indexable    
        String name;
      
        @Indexable    
        RecordLabel label;
    
        String genre;
    
        Date release;
    }
    
    static class RecordLabel { 
        @Indexable    
        String name;
    
        String email;
        String phone;
        String address;
    }
    
    static class QueryExecutionListener extends StorageListener
    {
        int nSequentialSearches;
        int nSorts;

        public void sequentialSearchPerformed(Class table, String query) {
            nSequentialSearches += 1;
        }        
    
        public void sortResultSetPerformed(Class table, String query) {
            nSorts += 1;
        }        
    }

    static final int nLabels = 100;
    static final int nAlbums = 10000;
    static final int nTracksPerAlbum = 10;
    
    public static void main(String[] args) 
    { 
        Storage storage = StorageFactory.getInstance().createStorage(); 
        storage.open("testcodegenerator.dbs");
        Database db = new Database(storage);

        long start = System.currentTimeMillis();

        for (int i = 0; i < nLabels; i++) { 
            RecordLabel label = new RecordLabel();
            label.name = "Label" + i;
            label.email = "contact@" + label.name + ".com";
            label.address = "Country, City, Street";
            label.phone = "+1 123-456-7890";
            db.addRecord(label);
        }        

        for (int i = 0; i < nAlbums; i++) { 
            Album album = new Album();
            album.name = "Album" + i;
            album.label = db.<RecordLabel>select(RecordLabel.class, "name='Label" + (i % nLabels) + "'").first();
            album.genre = "Rock";
            album.release = new Date();
            db.addRecord(album);
            
            for (int j = 0; j < nTracksPerAlbum; j++) { 
                Track track = new Track();
                track.no = j+1;
                track.name = "Track" + j;
                track.album = album;
                track.duration = 3.5f;
                db.addRecord(track);                
            }
        }
        storage.commit();
        System.out.println("Elapsed time for database initialization: " + (System.currentTimeMillis() - start));

        QueryExecutionListener listener = new QueryExecutionListener();
        storage.setListener(listener);
        Query<Track> trackQuery = db.<Track>createQuery(Track.class);
        CodeGenerator code = trackQuery.getCodeGenerator();
        code.predicate(code.and(code.gt(code.field("no"), 
                                        code.literal(0)), 
                                code.eq(code.field(code.field(code.field("album"), "label"), "name"),
                                        code.parameter(1, String.class))));
        start = System.currentTimeMillis();
        int nTracks = 0;
        for (int i = 0; i < nLabels; i++) {
            trackQuery.setParameter(1, "Label" + i);
            for (Track t : trackQuery) { 
                nTracks += 1;
            }
        }
        System.out.println("Elapsed time for searching of " + nTracks + " tracks: " + (System.currentTimeMillis() - start));
        Assert.that(nTracks == nAlbums*nTracksPerAlbum);

        String prev = "";
        int n = 0;
        Query<RecordLabel> labelQuery = db.<RecordLabel>createQuery(RecordLabel.class);
        code = labelQuery.getCodeGenerator();
        code.orderBy("name");
        for (RecordLabel label : labelQuery)
        {
            Assert.that(prev.compareTo(label.name) < 0);
            prev = label.name;
            n += 1;
        }
        Assert.that(n == nLabels);

        n = 0;
        prev = "";
        code = labelQuery.getCodeGenerator();
        code.predicate(code.like(code.field("name"), 
                                 code.literal("Label%")));
        code.orderBy("name");
        for (RecordLabel label : labelQuery)
        {
            Assert.that(prev.compareTo(label.name) < 0);
            prev = label.name;
            n += 1;
        }
        Assert.that(n == nLabels);

        n = 0;
        code = labelQuery.getCodeGenerator();
        code.predicate(code.in(code.field("name"), 
                               code.list(code.literal("Label1"), code.literal("Label2"), code.literal("Label3"))));
        for (RecordLabel label : labelQuery)
        {
            n += 1;
        }
        Assert.that(n == 3);

        n = 0; 
        code = labelQuery.getCodeGenerator();
        code.predicate(code.and(code.or(code.eq(code.field("name"),
                                                code.literal("Label1")),
                                        code.or(code.eq(code.field("name"),
                                                        code.literal("Label2")),
                                                code.eq(code.field("name"),
                                                        code.literal("Label3")))),
                                code.like(code.field("email"),
                                          code.literal("contact@%"))));
        for (RecordLabel label : labelQuery)
        {
            n += 1;
        }
        Assert.that(n == 3);

        code = labelQuery.getCodeGenerator();
        code.predicate(code.and(code.like(code.field("phone"),
                                          code.literal("+1%")),
                                code.in(code.field("name"), 
                                        code.parameter(1, ArrayList.class))));
        ArrayList<String> list = new ArrayList<String>(nLabels);
        for (int i = 0; i < nLabels; i++) {
            list.add("Label" + i);
        }
        n = 0;
        labelQuery.setParameter(1, list);
        for (RecordLabel label : labelQuery) { 
            Assert.that(label.name.equals("Label" + n++));
        }
        Assert.that(n == nLabels);        
        
        n = 0;
        code = trackQuery.getCodeGenerator();
        code.predicate(code.or(code.eq(code.field(code.field(code.field("album"), "label"), "name"),
                                       code.literal("Label1")),
                               code.eq(code.field(code.field(code.field("album"), "label"), "name"),
                                       code.literal("Label2"))));
        for (Track track : trackQuery)
        {
            Assert.that(track.album.label.name.equals("Label1") || track.album.label.name.equals("Label2"));
            n += 1;
        }
        Assert.that(n == nAlbums*nTracksPerAlbum*2/nLabels);

        Assert.that(listener.nSequentialSearches == 0);
        Assert.that(listener.nSorts == 0);

        db.dropTable(Track.class);
        db.dropTable(Album.class);
        db.dropTable(RecordLabel.class);

        storage.close();
    }
}        