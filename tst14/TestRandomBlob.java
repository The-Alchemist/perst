import org.garret.perst.*;

import java.util.Arrays;

public class TestRandomBlob
{ 
    final static int nIterations = 10000;
    final static long fileSize = 8L*1024*1024*1024;
    final static int maxRecordSize = 1000;
    static int pagePoolSize = 32*1024*1024;

    static public void main(String[] args) throws Exception
    {   
        Storage db = StorageFactory.getInstance().createStorage();
        Access[] map = initializeRandomAccessMap();
        db.open("testrndblob.dbs", pagePoolSize);
        Blob blob = (Blob)db.getRoot();
        if (blob == null) { 
            blob = db.createRandomAccessBlob();
            db.setRoot(blob);            
            storeContent(map, blob);
        }
        inspectContent(map, blob);
        db.close();
    }

    static class Access implements Comparable { 
        long pos;
        int  size;

        public int compareTo(Object o) { 
            long diff = pos - ((Access)o).pos;
            return diff < 0 ? -1 : diff == 0 ? 0 : 1;
        }
    }

    static Access[] initializeRandomAccessMap()
    {
        Access[] map = new Access[nIterations];
        Access[] sortedMap = new Access[nIterations];
        long key = 1999;
        for (int i = 0; i < nIterations; i++) { 
            map[i] = new Access();
            key = (3141592621L*key + 2718281829L) % 1000000007L;
            map[i].pos = key % fileSize;
            key = (3141592621L*key + 2718281829L) % 1000000007L;
            map[i].size = (int)(key % maxRecordSize);
            sortedMap[i] = map[i];
        }
        Arrays.sort(sortedMap);
        for (int i = 1; i < sortedMap.length; i++) { 
            if (sortedMap[i-1].pos + sortedMap[i-1].size > sortedMap[i].pos) { 
                sortedMap[i-1].size = (int)(sortedMap[i].pos - sortedMap[i-1].pos);
            }
        }
        return map;
    }
        
    static void inspectContent(Access[] map, Blob blob) throws Exception
    {
        RandomAccessInputStream in = blob.getInputStream();
        for (int i = 0; i < nIterations; i++) { 
            int size = map[i].size;
            long pos = map[i].pos;
            byte filler = (byte)(pos & 0xFF);
            byte[] content = new byte[size];
            in.setPosition(pos);
            int rc = in.read(content, 0, size);
            Assert.that(rc == size);
            for (int j = 0; j < size; j++) { 
                if (content[j] != filler) { 
                    System.out.println(i + ": filler = " + filler + ", content[" + j + "]=" + content[j]);
                }
                Assert.that(content[j] == filler);
            }
        }
    }

    static void storeContent(Access[] map, Blob blob) throws Exception
    {
        RandomAccessOutputStream out = blob.getOutputStream(0);
        for (int i = 0; i < nIterations; i++) { 
            int size = map[i].size;
            long pos = map[i].pos;
            byte filler = (byte)(pos & 0xFF);
            byte[] content = new byte[size];
            for (int j = 0; j < size; j++) { 
                content[j] = filler;
            }
            out.setPosition(pos);
            out.write(content, 0, size);
        }
    }
}

            