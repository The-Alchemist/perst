import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.lucene.store.*;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

public class SearchEngine { 
    static final String[] INDEXED_FILE_TYPES = { "html", "htm", "txt", "c", "cpp", "java", "cs", "h" };
    static final int MAX_FILE_SIZE = 1024*1024;

    static int addFiles(IndexWriter writer, File dir) throws IOException {  
        File[] files = dir.listFiles();
        int added = 0;
        if (files != null) { 
            for (int i = 0; i < files.length; i++) { 
                File f = files[i];
                if (f.isDirectory()) { 
                    added += addFiles(writer, f);
                } else { 
                    String fileName = f.getName().toLowerCase();
                    for (int j = 0; j < INDEXED_FILE_TYPES.length; j++) { 
                        if (fileName.endsWith("." + INDEXED_FILE_TYPES[j])) {
                            if (f.length() <= MAX_FILE_SIZE) {
                                Document doc = new Document();
                                doc.add(new Field("path", f.getPath(), Field.Store.YES, Field.Index.UN_TOKENIZED));
                                doc.add(new Field("name", fileName, Field.Store.YES, Field.Index.UN_TOKENIZED));
                                doc.add(new Field("modified",
                                                  DateTools.timeToString(f.lastModified(), DateTools.Resolution.MINUTE),
                                                  Field.Store.YES, Field.Index.UN_TOKENIZED));
                                
                                doc.add(new Field("contents", new FileReader(f)));
                                
                                writer.addDocument(doc);
                                added += 1;
                            }
                            break;
                        }
                    }                
                }
            }
        }
        return added;
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
    
    public static void main(String[] args) throws IOException { 
        boolean create = true;
        for (int i = 1; i < args.length; i++) { 
            if (args[i].equals("-create")) { 
                create = true;
            }
        }
        Directory directory = FSDirectory.getDirectory("index", true);
        StandardAnalyzer analyzer = new StandardAnalyzer();
        while (true) { 
            switch (inputInt("-------------------------------------\n" + 
                             "Menu:\n" + 
                             "1. Index files\n" + 
                             "2. Search\n" + 
                             "3. Exit\n\n>>"))
            {
            case 1:                    
            {
                long start = System.currentTimeMillis();
                IndexWriter writer = new IndexWriter(directory, analyzer, create);
                int nFiles = addFiles(writer, new File(input("Root directory: ")));
                writer.optimize();
                writer.close();
                System.out.println(nFiles + " files are imported in " 
                                   + (System.currentTimeMillis() - start) + " milliseconds");
                
                break;
            }
            case 2:
            {
                long start = System.currentTimeMillis();
                IndexSearcher searcher = new IndexSearcher(directory);
                QueryParser parser = new QueryParser("contents", analyzer);
                try { 
                    Query query = parser.parse(input("Query: "));
                    Hits hits = searcher.search(query);
                    for (int i = 0; i < hits.length(); i++) { 
                        System.out.println(hits.score(i) + "\t" + hits.doc(i).get("path"));
                    }
                    System.out.println("Elapsed search time for " + hits.length() + " matched resuts: " + (System.currentTimeMillis() - start) + " milliseconds");
                } catch (ParseException x) { 
                    System.err.println("Invalid query: " + x);
                }
                searcher.close();
                break;
            }
            case 3:
            {
                return;
            }
            }
            skip("Press ENTER to continue...");
        }
    }
}        
        