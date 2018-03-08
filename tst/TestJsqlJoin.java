import org.garret.perst.*;

import java.util.Date;
import java.util.ArrayList;

class Track { 
    int no;

    @Indexable
    Album album;

    @Indexable    
    String name;

    float duration;
}

class Album { 
    @Indexable(unique=true, caseInsensitive=true)    
    String name;
  
    @Indexable    
    RecordLabel label;

    @Indexable(thick=true, caseInsensitive=true)   
    String genre;

    Date release;
}

class RecordLabel { 
    @Indexable(unique=true)      
    String name;

    String email;
    String phone;
    String address;
}

class QueryExecutionListener extends StorageListener
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


public class TestJsqlJoin 
{ 
    static final int nLabels = 100;
    static final int nAlbums = 10000;
    static final int nTracksPerAlbum = 10;
    
    static final String GENRES[] = {"Rock", "Pop", "Jazz", "R&B", "Folk", "Classic"};

    public static void main(String[] args) 
    { 
        Storage storage = StorageFactory.getInstance().createStorage(); 
        storage.open("testjsqljoin.dbs");
        storage.getSqlOptimizerParameters().enableCostBasedOptimization = true;
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
            album.genre = GENRES[i % GENRES.length];
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
        Query<Track> query = db.<Track>prepare(Track.class, "no > 0 and album.label.name=?");
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
        prev = "zzz";
        for (RecordLabel label : db.<RecordLabel>select(RecordLabel.class, "name like 'Label%' order by name desc"))
        {
            Assert.that(prev.compareTo(label.name) > 0);
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
        String labelName = "Label2";
        for (Track track : db.<Track>select(Track.class, "album.label.name='Label1' or album.label.name='Label2' order by album.label.name desc"))
        {            
            Assert.that(track.album.label.name.equals(labelName) || track.album.label.name.equals(labelName = "Label1"));
            n += 1;
        }
        Assert.that(n == nAlbums*nTracksPerAlbum*2/nLabels);

        Query<Album> query3 = db.<Album>prepare(Album.class, "label=?");
        n = 0;
        for (RecordLabel label : db.<RecordLabel>getRecords(RecordLabel.class)) {
            query3.setParameter(1, label);
            for (Album album : query3) { 
                n += 1;
            }
        }
        Assert.that(n == nAlbums);
                  
        n = 0;
        for (Album album : db.<Album>select(Album.class, "name in ('Album1', 'Album2', 'Album3', 'Album4', 'Album5')")) {
            n += 1;
            Assert.that(album.name.equals("Album" + n));
        }
        Assert.that(n == 5);

        Query<Album> query4 = db.<Album>prepare(Album.class, "genre in ?");
        ArrayList<String> genres = new ArrayList<String>();
        for (String genre : GENRES) { 
            genres.add(genre.toLowerCase());
        }
        query4.setParameter(1, genres);
        n = 0;
        for (Album album : query4) {
            n += 1;
        }
        Assert.that(n == nAlbums);
        
        Assert.that(listener.nSequentialSearches == 0);
        Assert.that(listener.nSorts == 1);

        db.dropTable(Track.class);
        db.dropTable(Album.class);
        db.dropTable(RecordLabel.class);

        storage.close();
    }
}        