import java.io.File ;
import java.io.IOException ;

import org.garret.perst.*;

import org.apache.lucene.store.*;
import org.apache.lucene.index.IndexWriter ;
import org.apache.lucene.analysis.standard.StandardAnalyzer ;
import org.apache.lucene.document.Document ;
import org.apache.lucene.document.Field ;

public class PerstIndexer {

    static final int pagePoolSize = 64*1024*1024;
    
    static final int nDocuments = 10000;
    static final int nFields = 10;
    static final int nWords = 10;

    public static void main(String[] args) throws IOException
    {
        if (args.length < 1)
        {
            System.err.println("Usage: PerstIndexer <index dir> [-create] [-fs]");
            System.exit(-1);
        }

        String indexDir = args[0];
        boolean create = false;
        boolean fileSystem = false;
        for (int i = 1; i < args.length; i++) { 
            if (args[i].equals("-create")) { 
                create = true;
            } else if (args[i].equals("-fs")) { 
                fileSystem = true;
            }
        }
        File dbHome = new File(indexDir);
        File dbFile = new File(dbHome, "index.dbs");

        if (!dbHome.exists()) { 
            dbHome.mkdir();
        } else if (create) {
            dbFile.delete();
        }
        Storage db = StorageFactory.getInstance().createStorage();
        db.open(dbFile.getPath(), pagePoolSize);
        Directory directory = fileSystem 
            ? (Directory)FSDirectory.getDirectory("index", true)
            : (Directory)new PerstDirectory(db);
        try {
            IndexWriter writer = new IndexWriter(directory, new StandardAnalyzer(), create);
            writer.setUseCompoundFile(false); // this method is absent in Lucene 1.2
            WordGenerator generator = new WordGenerator();
            for (int i = 0; i < nDocuments; i++) { 
                Document doc = new Document();
                for (int j = 0; j < nFields; j++) { 
                    StringBuffer buf = new StringBuffer();
                    for (int k = 0; k < nWords; k++) { 
                        generator.generateWord(buf);
                        buf.append(' ');
                    }
                    doc.add(new org.apache.lucene.document.Field
                            ("Field" + j, 
                             buf.toString(),
                             org.apache.lucene.document.Field.Store.YES, 
                             org.apache.lucene.document.Field.Index.TOKENIZED));
                }
                writer.addDocument(doc);
            }            

            writer.optimize();
            writer.close();
            db.close();
        } catch (IOException e) {
            db.rollback();
            throw e;
        } catch (StorageError e) {
            db.rollback();
            throw e;
        }
        System.out.println("Indexing Complete");
    }
}
