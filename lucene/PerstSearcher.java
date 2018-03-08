import org.garret.perst.*;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.*;

import java.io.File;
import java.io.IOException;

public class PerstSearcher {

    static final int pagePoolSize = 64*1024*1024;

    static final int nDocuments = 10000;
    static final int nFields = 10;
    static final int nWords = 10;

    public static void main(String[] args) throws IOException
    {
        if (args.length == 0)
        {
            System.err.println("Usage: PerstSearcher <index dir> [-fs]");
            System.exit(-1);
        }
        boolean fileSystem = false;
        for (int i = 1; i < args.length; i++) { 
            if (args[i].equals("-fs")) { 
                fileSystem = true;
            }
        }
        File dbHome = new File(args[0]);
        File dbFile = new File(dbHome, "index.dbs");
        Storage db = StorageFactory.getInstance().createStorage();
        db.open(dbFile.getPath(), pagePoolSize);
        Directory directory = fileSystem 
            ? (Directory)FSDirectory.getDirectory("index", false)
            : (Directory)new PerstDirectory(db);

        IndexSearcher searcher;

        try {
            searcher = new IndexSearcher(directory);

            WordGenerator generator = new WordGenerator();
            for (int i = 0; i < nDocuments; i++) { 
                for (int j = 0; j < nFields; j++) { 
                    for (int k = 0; k < nWords; k++) { 
                        StringBuffer buf = new StringBuffer();
                        generator.generateWord(buf);
                        Hits hits = searcher.search(new TermQuery(new Term("Field" + j, buf.toString())));
                        Assert.that(hits.length() > 0);
                    }
                }
            }
            searcher.close();
        } finally {
            db.close();
        }
    }
}
