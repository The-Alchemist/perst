import org.garret.perst.*;
import org.garret.perst.fulltext.*;

import java.io.*;
import java.util.*;

public class TestFullTextIndex 
{ 
    static final String LANGUAGE = "en";
    static final int SEARCH_TIME_LIMIT = 2*1000; // 2 seconds

    static class SourceFile extends Persistent implements FullTextSearchable 
    { 
        String name;
        long   lastModified;
        Blob   body;
    
        public Reader getText() { 
            return new InputStreamReader(body.getInputStream());
        }
    
        public String getLanguage() { 
            return LANGUAGE;
        }

        SourceFile(Storage storage, File f) throws IOException { 
            super(storage);
            name = f.getName();
            lastModified = f.lastModified();
            body = storage.createBlob();
            OutputStream out = body.getOutputStream(Blob.TRUNCATE_LAST_SEGMENT);
            InputStream in = new FileInputStream(f);
            byte[] buf = new byte[64*1024];
            int rc;
            while ((rc = in.read(buf, 0, buf.length)) > 0) {
                out.write(buf, 0, rc);
            }
            out.close();
            in.close();
        }

        SourceFile() {}
    }
            
    static class Project extends Persistent { 
        FullTextIndex index;
        FieldIndex    sources;

        Project(Storage storage) { 
            super(storage);
            this.index = storage.createFullTextIndex();
            this.sources = storage.createFieldIndex(SourceFile.class, "name", true);
        }
                                                
        Project() {}
    }

    static void printResult(FullTextSearchResult result) { 
        for (int i = 0; i < result.hits.length; i++) { 
            System.out.println(result.hits[i].rank + "\t" + ((SourceFile)result.hits[i].getDocument()).name);
        }
    }

    public static void main(String[] args) throws IOException 
    { 
        boolean reload = args.length > 0 && "reload".equals(args[0]);

        Storage db = StorageFactory.getInstance().createStorage();
        db.open("testfulltext.dbs");

        Project project = (Project)db.getRoot();
        if (project == null) { 
            project = new Project(db);
            db.setRoot(project);
        }
        if (project.sources.size() == 0 || reload) { 
            File dir = new File(".");
            File[] files = dir.listFiles();
            long start = System.currentTimeMillis();
            for (int i = 0; i < files.length; i++) { 
                String fileName = files[i].getName();
                if (fileName.endsWith(".java")) {
                    SourceFile file = (SourceFile)project.sources.get(fileName);
                    if (file == null) { 
                        file = new SourceFile(db, files[i]);
                        project.sources.put(file);
                    }
                    project.index.add(file);
                }
            }
            System.out.println(project.sources.size() + " files are imported to the project in " + (System.currentTimeMillis() - start) + " milliseconds");
        } 
        FullTextSearchResult result;
        long start = System.currentTimeMillis();
        System.out.println("Total documents: " + project.index.getNumberOfDocuments() + " with " + project.index.getNumberOfWords() + " unique words");
        Assert.that(project.index.getNumberOfDocuments() == project.sources.size());

        result = project.index.search("persistent capable class", LANGUAGE, 100, SEARCH_TIME_LIMIT);
        Assert.that(result.estimation == 2 
                    && result.hits.length == 2
                    && ((SourceFile)result.hits[0].getDocument()).name.equals("Simple.java")
                    && ((SourceFile)result.hits[1].getDocument()).name.equals("TestFullTextIndex.java"));


        result = project.index.search("class", LANGUAGE, 10, SEARCH_TIME_LIMIT);
        Assert.that(result.estimation == project.sources.size()
                    && result.hits.length == 10);

        result = project.index.search("MultidimensionalIndex OR SpatialIndex", LANGUAGE, 10, SEARCH_TIME_LIMIT);
        Assert.that(result.estimation == 4 && result.hits.length == 4);

        result = project.index.search("(MultidimensionalIndex AND MultidimensionalComparator) OR (Blob AND registerCustomAllocator)", LANGUAGE, 10, SEARCH_TIME_LIMIT);
        Assert.that(result.estimation == 3 && result.hits.length == 3);
        
        result = project.index.search("(MultidimensionalIndex AND Single" + "dimensionalIndex) OR (Blob AND registerCustomAllocator)", LANGUAGE, 10, SEARCH_TIME_LIMIT);
        Assert.that(result.estimation == 2 && result.hits.length == 2);
        
        result = project.index.search("BLOB AND NOT createBitmapAllocator", LANGUAGE, 10, SEARCH_TIME_LIMIT);
        Assert.that(result.estimation == 2 && result.hits.length == 2
                    && ((SourceFile)result.hits[0].getDocument()).name.equals("TestRandomBlob.java")
                    && ((SourceFile)result.hits[1].getDocument()).name.equals("TestBlob.java"));

        result = project.index.search("main", LANGUAGE, 1000, SEARCH_TIME_LIMIT);
        Assert.that(result.estimation == result.hits.length 
                    && project.sources.size() == result.hits.length);

        result = project.index.search("public FieldIndex", LANGUAGE, 100, SEARCH_TIME_LIMIT);
        printResult(result);
        Assert.that(result.hits.length >= 2 && result.estimation >= result.hits.length
                    && ((SourceFile)result.hits[0].getDocument()).name.equals("TestFullTextIndex.java")
                    && ((SourceFile)result.hits[1].getDocument()).name.equals("TestDynamicObjects.java"));

        result = project.index.search("\"public FieldIndex\"", LANGUAGE, 100, SEARCH_TIME_LIMIT);
        Assert.that(result.hits.length == 2 && result.estimation == 2
                    && ((SourceFile)result.hits[0].getDocument()).name.equals("TestFullTextIndex.java")
                    && ((SourceFile)result.hits[1].getDocument()).name.equals("Simple.java"));

        result = project.index.search("Multi" + "Platform", LANGUAGE, 100, SEARCH_TIME_LIMIT);
        Assert.that(result.estimation == 0 && result.hits.length == 0);
        
        result = project.index.search("getRoot AND NOT null", LANGUAGE, 100, SEARCH_TIME_LIMIT);
        Assert.that(result.estimation == 0 && result.hits.length == 0);
        
        result = project.index.search("\"MyPersistentClass extends MyRootClass\"", LANGUAGE, 100, SEARCH_TIME_LIMIT);
        Assert.that(result.estimation == 1 && result.hits.length == 1
                    && ((SourceFile)result.hits[0].getDocument()).name.equals("TestFullTextIndex.java"));
                    
        result = project.index.search("to be or not to be", LANGUAGE, 100, SEARCH_TIME_LIMIT);
        Assert.that(result.estimation == 2 && result.hits.length == 2
                    && ((SourceFile)result.hits[0].getDocument()).name.equals("TestFullTextIndex.java")
                    && ((SourceFile)result.hits[1].getDocument()).name.equals("Simple.java"));
                           
        result = project.index.search("\"to be or not to be\"", LANGUAGE, 100, SEARCH_TIME_LIMIT);
        Assert.that(result.estimation == 1 && result.hits.length == 1
                    && ((SourceFile)result.hits[0].getDocument()).name.equals("TestFullTextIndex.java"));
                    
        result = project.index.search("class", LANGUAGE, 0, 0);
        Assert.that(result.estimation == project.sources.size() && result.hits.length == 0);
                    
        result = project.index.search("class public static void main string perst open", LANGUAGE, 1000, SEARCH_TIME_LIMIT);
        Assert.that(result.estimation == result.hits.length 
                    && project.sources.size() == result.hits.length+1);
        System.out.println("Elapsed time for full text searches: " + (System.currentTimeMillis() - start) + " milliseconds");

        db.close();
    }
}

