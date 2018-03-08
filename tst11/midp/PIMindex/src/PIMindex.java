import javax.microedition.midlet.*;
import javax.microedition.lcdui.*;
import java.io.*;
import java.util.*;
import org.garret.perst.*;
import org.garret.perst.fulltext.*;
import javax.microedition.pim.*;

public class PIMindex extends MIDlet implements CommandListener  
{ 
    static Command QUIT_CMD = new Command("Quit", Command.EXIT, 3);
    static Command BACK_CMD = new Command("Back", Command.BACK, 3);
    static Command FIND_CMD = new Command("Find", Command.OK, 1);
    static Command REFRESH_CMD = new Command("Refresh", Command.OK, 2);
    
    static final String DATABASE_NAME = "pimindex.dbs";
    static final String LANGUAGE = "en";
    static final int PAGE_POOL_SIZE = Storage.INFINITE_PAGE_POOL;
    static final int MAX_RESULTS = 10;
    static final int TIME_LIMIT = 2*1000; // two seconds

    protected void destroyApp(boolean unconditional) 
    {
        // When application is terminated, do not forget to close the database
        db.close();
        searchForm = null;
    }    

    protected  void pauseApp() 
    {
    }

    protected void startApp() 
    {
        searchForm = new Form("PIM index");
        inputField = new TextField("Find", "", 100, TextField.ANY);
        searchForm.append(inputField);
        Display.getDisplay(this).setCurrent(searchForm);
        searchForm.addCommand(PIMindex.FIND_CMD);
        searchForm.addCommand(PIMindex.REFRESH_CMD);
        searchForm.addCommand(PIMindex.QUIT_CMD);
        searchForm.setCommandListener(this);
    }

    public void commandAction(Command c, Displayable d) 
    {
        if (c == QUIT_CMD) {
            destroyApp(true);
            notifyDestroyed();
        } else if (c == FIND_CMD) {
            String query = inputField.getString().trim();
            if (query.length() != 0) { 
                new SearchResultList(this, query, index.search(query, LANGUAGE, MAX_RESULTS, TIME_LIMIT));
            }
        } else if (c == REFRESH_CMD) { 
            // reconstruct the index
            index.clear();
            importData();
            db.commit();
        }
    }

    void importData() 
    { 
        try { 
            PIM pim = PIM.getInstance();
            String[] allLists = pim.listPIMLists(PIM.CONTACT_LIST);
            for (int i = 0; i < allLists.length; i++) { 
                ContactList list = (ContactList)pim.openPIMList(PIM.CONTACT_LIST, PIM.READ_ONLY, allLists[i]);
                Enumeration items = list.items();
                while (items.hasMoreElements()) { 
                    Contact contact = (Contact)items.nextElement();
                    index.add(new ContactDetails(contact));
                }           
            }
        } catch (PIMException x) { 
            x.printStackTrace();
        }
    }

    public PIMindex() 
    {
        // Get instance of Perst storage
        db = StorageFactory.getInstance().createStorage();
        db.setProperty("perst.string.encoding", "UTF-8");  // use UTF-8 encoding for strings
        // Open the database with given database name and specified page pool (database cache) size
        db.open(DATABASE_NAME, PAGE_POOL_SIZE);
        index = (FullTextIndex)db.getRoot(); // full text index is root object of the storage
        if (index == null) { // if database is not yet initialized
            // create full text search index using default FullTextSearchHelper implementation 
            index = FullTextIndexFactory.createFullTextIndex(db); 
            db.setRoot(index); // set new root object
            importData(); // import data from PIM to the Perst database
            db.commit(); // commit transaction, saving all imported data in the database
        }
    }                



    FullTextIndex index;
    Storage db;
    Form searchForm;
    TextField inputField;
}