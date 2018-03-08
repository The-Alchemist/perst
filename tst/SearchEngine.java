// Full text search engine example

import org.garret.perst.*;
import org.garret.perst.fulltext.*;
import java.util.*;
import java.io.*;

public class SearchEngine { 
    static final int PAGE_POOL_SIZE = 256*1024*1024;
    static final String LANGUAGE = "en";
    static final int MAX_RESULTS = 1000; 
    static final int SEARCH_TIME_LIMIT = 2*1000; // 2 seconds
    static final String[] INDEXED_FILE_TYPES = { "html", "htm", "txt", "c", "cpp", "java", "cs", "h" };
    static final int MAX_FILE_SIZE = 1024*1024;
    static final String CIPHER_KEY = "0123456789";

    static class Workspace extends Persistent { 
        FullTextIndex index;
        FieldIndex    files;

        Workspace(Storage storage) { 
            super(storage);
            this.index = storage.createFullTextIndex();
            this.files = storage.createFieldIndex(TextFile.class, "path", true);
        }
                                                
        Workspace() {}
    }

    static class TextFile extends Persistent
    { 
        String path;
        long   lastModified;
    
        TextFile(File f) throws IOException { 
            path = f.getPath();
            lastModified = f.lastModified();
        }

        TextFile() {}
    }

    static void addFiles(Workspace ws, File dir) throws IOException {  
        File[] files = dir.listFiles();
        if (files != null) { 
            for (int i = 0; i < files.length; i++) { 
                File f = files[i];
                if (f.isDirectory()) { 
                    addFiles(ws, f);
                } else { 
                    String fileName = f.getName().toLowerCase();
                    for (int j = 0; j < INDEXED_FILE_TYPES.length; j++) { 
                        if (fileName.endsWith("." + INDEXED_FILE_TYPES[j])) { 
                            TextFile file = (TextFile)ws.files.get(f.getPath());
                            if (file == null) { 
                                file = new TextFile(f);
                                ws.files.put(file);
                            }
                            if (f.length() <= MAX_FILE_SIZE) { 
                                FileReader reader = new FileReader(f);
                                //System.out.println("Index file " + f.getPath() + ", free memory " + Runtime.getRuntime().freeMemory());
                                ws.index.add(file, reader, LANGUAGE);
                                reader.close();
                            }
                            break;
                        }
                    }                
                }
            }
        }
    }

    static byte[] inputBuffer = new byte[256];

    static void skip(String prompt) {
        try { 
            System.out.print(prompt);
            System.in.read(inputBuffer);
        } catch (IOException x) {}
    }

    static String input(String prompt) {
        while (true) { 
            try { 
                System.out.print(prompt);
                int len = System.in.read(inputBuffer);
                String answer = new String(inputBuffer, 0, len).trim();
                if (answer.length() != 0) {
                    return answer;
                }
            } catch (IOException x) {}
        }
    }

    static int inputInt(String prompt) { 
        while (true) { 
            try { 
                return Integer.parseInt(input(prompt), 10);
            } catch (NumberFormatException x) { 
                System.err.println("Invalid integer constant");
            }
        }
    }
    
    public static void main(String[] args) { 
        Storage db = StorageFactory.getInstance().createStorage();
        //db.open("searchengine.dbs", PAGE_POOL_SIZE);        
        db.open(new CompressedReadWriteFile("searchengine.dbs", CIPHER_KEY), PAGE_POOL_SIZE);        
        Workspace ws = (Workspace)db.getRoot();
        if (ws == null) { 
            ws = new Workspace(db);
            db.setRoot(ws);
        }
        if (args.length != 0) { 
            long start = System.currentTimeMillis();
            int nFiles = ws.files.size();
            for (int i = 0; i < args.length; i++) { 
                try { 
                    addFiles(ws, new File(args[i]));
                } catch (IOException x) { 
                    System.err.println("Catch exception " + x);
                }
            }
            db.commit();
            System.out.println((ws.files.size() - nFiles) + " files are imported to the workspace in " 
                               + (System.currentTimeMillis() - start) + " milliseconds");
        }
        while (true) { 
            try { 
                switch (inputInt("-------------------------------------\n" + 
                                 "Menu:\n" + 
                                 "1. Index files\n" + 
                                 "2. Search\n" + 
                                 "3. Statistic\n" + 
                                 "4. Exit\n\n>>"))
                {
                case 1:                    
                {
                    long start = System.currentTimeMillis();
                    int nFiles = ws.files.size();
                    try { 
                        addFiles(ws, new File(input("Root directory: ")));
                        db.commit();
                    } catch (IOException x) { 
                        System.err.println("Catch exception " + x);
                    }
                    System.out.println((ws.files.size() - nFiles) + " files are imported to the workspace in " 
                                       + (System.currentTimeMillis() - start) + " milliseconds");
            
                    break;
                }
                case 2:
                {
                    long start = System.currentTimeMillis();
                    FullTextSearchResult result = ws.index.search(input("Query: "), LANGUAGE, MAX_RESULTS, SEARCH_TIME_LIMIT);
                    for (int i = 0; i < result.hits.length; i++) { 
                        System.out.println(result.hits[i].rank + "\t" + ((TextFile)result.hits[i].getDocument()).path);
                    }
                    System.out.println("Elapsed search time for " + result.hits.length + " matched resuts: " + (System.currentTimeMillis() - start) + " milliseconds");
                    
                    break;
                }
                case 3:
                {
                    System.out.println("Number of indexed documents: " + ws.index.getNumberOfDocuments());
                    System.out.println("Number total number of words: " + ws.index.getNumberOfWords());
                    break;
                }
                case 4:
                {
                    db.close();
                    return;
                }
                }
            } catch (StorageError x) { 
                System.out.println("Error: " + x.getMessage());
            }
            skip("Press ENTER to continue...");
        }
    }
}        
        