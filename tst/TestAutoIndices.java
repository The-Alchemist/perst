import org.garret.perst.*;

import java.util.Date;
import java.util.ArrayList;

public class TestAutoIndices { 
    static class Track { 
        int no;
        Album album;
        String name;
        float duration;
    }
    
    static class Album { 
        String name;
        RecordLabel label;
        String genre;    
        Date release;
    }
    
    static class RecordLabel { 
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
        storage.open("testautoindices.dbs");
        Database db = new Database(storage);

        boolean wasEnabled = db.enableAutoIndices(true);

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
        Query<Track> query = db.<Track>prepare(Track.class, "album.label.name=?");
        start = System.currentTimeMillis();
        int nTracks = 0;
        for (int i = 0; i < nLabels; i++) {
            query.setParameter(1, "Label" + i);
            for (Track t : query) { 
                nTracks += 1;
            }
        }
        System.out.println("Elapsed time for searching of " + nTracks + " tracks: " + (System.currentTimeMillis() - start));
        Assert.that(nTracks == nAlbums*nTracksPerAlbum);

        String prev = "";
        int n = 0;
        for (RecordLabel label : db.<RecordLabel>select(RecordLabel.class, "order by name"))
        {
            Assert.that(prev.compareTo(label.name) < 0);
            prev = label.name;
            n += 1;
        }
        Assert.that(n == nLabels);

        n = 0;
        prev = "";
        for (RecordLabel label : db.<RecordLabel>select(RecordLabel.class, "name like 'Label%' order by name"))
        {
            Assert.that(prev.compareTo(label.name) < 0);
            prev = label.name;
            n += 1;
        }
        Assert.that(n == nLabels);

        n = 0;
        for (RecordLabel label : db.<RecordLabel>select(RecordLabel.class, "name in ('Label1', 'Label2', 'Label3')"))
        {
            n += 1;
        }
        Assert.that(n == 3);

        n = 0;
        for (RecordLabel label : db.<RecordLabel>select(RecordLabel.class, "(name = 'Label1' or name = 'Label2' or name = 'Label3') and email like 'contact@%'"))
        {
            n += 1;
        }
        Assert.that(n == 3);

        Query<RecordLabel> query2 = db.<RecordLabel>prepare(RecordLabel.class, "phone like '+1%' and name in ?");
        ArrayList<String> list = new ArrayList<String>(nLabels);
        for (int i = 0; i < nLabels; i++) {
            list.add("Label" + i);
        }
        n = 0;
        query2.setParameter(1, list);
        for (RecordLabel label : query2) { 
            Assert.that(label.name.equals("Label" + n++));
        }
        Assert.that(n == nLabels);        
        
        n = 0;
        for (Track track : db.<Track>select(Track.class, "album.label.name='Label1' or album.label.name='Label2'"))
        {
            Assert.that(track.album.label.name.equals("Label1") || track.album.label.name.equals("Label2"));
            n += 1;
        }
        Assert.that(n == nAlbums*nTracksPerAlbum*2/nLabels);

        Assert.that(listener.nSequentialSearches == 0);
        Assert.that(listener.nSorts == 0);

        // iterate through one of the indices and remove all records
        for (Track track : db.<Track>select(Track.class, "name like 'Track%'", true))
        {
            db.deleteRecord(track);
        }

        // iterate using table extent iterator and remove all records
        for (Album album : db.<Album>getRecords(Album.class, true))
        {
            db.deleteRecord(album);
        }

        // iterate through results of some sequntial query and remove all records 
        for (RecordLabel label : db.<RecordLabel>select(RecordLabel.class, "name like '%Track%'", true))
        {
            db.deleteRecord(label);
        }

        System.out.println("nSequentialSearches = " + listener.nSequentialSearches);
        Assert.that(listener.nSequentialSearches == 1);
 
        storage.close();
    }
}        
