import java.util.*;
import org.garret.perst.*;
import org.garret.perst.assoc.*;
import org.garret.perst.fulltext.*;

public class Library
{
    int nAuthors;
    int nCoauthors = 2;
    int nBooksPerAutor = 10;

    static final int MAX_FULL_TEXT_SEARCH_RESULTS = 1000;
    static final int MAX_FULL_TEXT_SEARCH_TIME = 10000; // 10 seconds

    static String generateWord(int x) 
    { 
        char[] chars = new char[8];
        for (int i = 0; i < 8; i++) { 
            chars[i] = (char)('A' + (x & 0xF));
            x >>>= 4;
        }
        return new String(chars);
    }

    static String generateTitle(int x)
    {
        return generateWord(x) + " " + generateWord(~x);
    }

    static String generateName(int x)
    {
        return "Mr" + generateWord(x) + " Mr" + generateWord(~x);
    }
            
    static String generateISBN(int x) 
    {
        return Integer.toString(x);
    }
        
    
    void populateDatabase()
    {
        long start = System.currentTimeMillis();
        ReadWriteTransaction t = db.startReadWriteTransaction();
        
        if (t.getVerbs().size() == 0) { 
            int nBooks = nAuthors * nBooksPerAutor / nCoauthors;
            Item[] author = new Item[nAuthors];
            for (int i = 0; i < nAuthors; i++) { 
                author[i] = t.createItem();
                t.link(author[i], "name",  generateName(i));
                t.includeInFullTextIndex(author[i]);
            }
            for (int i = 0, j = 0; i < nBooks; i++) { 
                Item book = t.createItem();
                t.link(book, "title", generateTitle(i));
                t.link(book, "ISBN", generateISBN(i));
                t.link(book, "publish-date", (new Date()).getTime());
                t.includeInFullTextIndex(book, new String[]{"title", "ISBN"});
                for (int k = 0; k < nCoauthors; k++) { 
                    t.link(book, "author", author[j++ % nAuthors]);
                }
            }
            System.out.println("Elapsed time for populating database " + (System.currentTimeMillis() - start) + " millisconds");
        }
        t.commit();
    }
    
    void searchDatabase() 
    {
        ReadOnlyTransaction t = db.startReadWriteTransaction();

        long start = System.currentTimeMillis();
        int nBooks = nAuthors * nBooksPerAutor / nCoauthors;
        for (int i = 0, j = 0; i < nBooks; i++) { 
            // find authors of the book
            ArrayList<Item> authors = t.find(Predicate.in("-author", Predicate.compare("title", Predicate.Compare.Operation.Equals, generateTitle(i)))).toList();
            Assert.that(authors.size() == nCoauthors);
            for (int k = 0; k < nCoauthors; k++) { 
                Assert.that(authors.get(k).getString("name").equals(generateName(j++ % nAuthors)));
            }
        }
        for (int i = 0, j = 0; i < nAuthors; i++) { 
            // find book written by this author
            ArrayList<Item> books = t.find(Predicate.in("author", Predicate.compare("name", Predicate.Compare.Operation.Equals, generateName(i)))).toList();
            Assert.that(books.size() == nBooksPerAutor);
        }
        System.out.println("Elapsed time for searching database " + (System.currentTimeMillis() - start) + " millisconds");

        start = System.currentTimeMillis();
        for (int i = 0, mask = 0; i < nBooks; i++, mask = ~mask) { 
            // find book using full text search part of book title and ISDN
            FullTextSearchResult result = t.fullTextSearch(generateWord(i ^ mask) + " " + generateISBN(i), MAX_FULL_TEXT_SEARCH_RESULTS, MAX_FULL_TEXT_SEARCH_TIME);
            Assert.that(result.hits.length == 1);
        }
        for (int i = 0, mask = 0; i < nAuthors; i++, mask = ~mask) { 
            // find authors using full text search of author's name
            FullTextSearchResult result = t.fullTextSearch(generateName(i ^ mask), MAX_FULL_TEXT_SEARCH_RESULTS, MAX_FULL_TEXT_SEARCH_TIME);
            Assert.that(result.hits.length == 1);
        }
        System.out.println("Elapsed time for full text search " + (System.currentTimeMillis() - start) + " millisconds");

        t.commit();
    }
            
    void shutdown()
    {
        storage.close();
    }

    Library(int authors)
    {
        nAuthors = authors;
        storage = StorageFactory.getInstance().createStorage();
        storage.open("library.dbs");
        db = new AssocDB(storage);
    }
     
    public static void main(String[] args) 
    {
        int nAuthors = args.length > 0 ? Integer.parseInt(args[0]) : 10000;
        Library library = new Library(nAuthors);
        library.populateDatabase();
        library.searchDatabase();
        library.shutdown();
    }
   
    AssocDB db;
    Storage storage;
}