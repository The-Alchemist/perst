package org.garret.perst.sample;

import android.app.Activity;
import android.content.Intent;
import android.database.Cursor;
import android.net.Uri;
import android.os.Bundle;
import android.widget.*;
import android.view.*;
import android.provider.Contacts.People;

import org.garret.perst.*;
import org.garret.perst.fulltext.*;

public class ContactsIndex extends Activity 
{
    static final private int FIND_CMD = Menu.FIRST;
    static final private int REFRESH_CMD = Menu.FIRST + 1;

    static final String DATABASE_NAME = "contacts.idx";
    static final String LANGUAGE = "en";
    static final int PAGE_POOL_SIZE = Storage.INFINITE_PAGE_POOL;
    static final int MAX_RESULTS = 10;
    static final int TIME_LIMIT = 2*1000; // two seconds
    
    EditText et;
    FullTextIndex index;
    Storage db;
    
    static ContactsIndex instance;
    
    /** Called when the activity is first created. */
    @Override
    public void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        instance = this;
        db = StorageFactory.getInstance().createStorage();
        db.setProperty("perst.string.encoding", "UTF-8");  // use UTF-8 encoding for strings
        String databasePath = DATABASE_NAME;
        try { 
            this.openFileOutput(databasePath, MODE_APPEND).close();
            databasePath = getFileStreamPath(databasePath).getAbsolutePath();	
        } catch (java.io.IOException x) {}
         // Open the database with given database name and specified page pool (database cache) size
        db.open(databasePath, PAGE_POOL_SIZE);
        index = (FullTextIndex)db.getRoot(); // full text index is root object of the storage
        if (index == null) { // if database is not yet initialized
            // create full text search index using default FullTextSearchHelper implementation 
            index = db.createFullTextIndex(); 
            db.setRoot(index); // set new root object
            importContacts(); // import data from PIM to the Perst database
            db.commit(); // commit transaction, saving all imported data in the database
        }
        setContentView(R.layout.main);
        
        // The text view for our note, identified by its ID in the XML file.
        et = (EditText) findViewById(R.id.title);
    }
 
    @Override
    public void onDestroy() {
    	db.close();
    	super.onDestroy();
    }
    
    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        super.onCreateOptionsMenu(menu);

        // We are going to create two menus. Note that we assign them
        // unique integer IDs, labels from our string resources, and
        // given them shortcuts.
        menu.add(0, FIND_CMD, 0, R.string.find); 
        menu.add(0, REFRESH_CMD, 0, R.string.refresh); 
        return true;
    }
    
    FullTextSearchResult search(String query) {
    	return index.search(query, LANGUAGE, MAX_RESULTS, TIME_LIMIT);
    }


    @Override
    public boolean onOptionsItemSelected(MenuItem item) {
        switch (item.getItemId()) {
        case FIND_CMD:
            String query = et.getText().toString();
            if (query.length() != 0) { 
            	Intent intent = new Intent(Intent.ACTION_SEARCH);
            	intent.setClass(this, SearchResultList.class);
            	intent.putExtra("query", query);
                startActivity(intent);
            }
        	break;
        case REFRESH_CMD:
            // reconstruct the index
            index.clear();
            importContacts();
            db.commit();
        	break;
        default:
            return super.onOptionsItemSelected(item);
        }
        return true;
    }
       
    
    void importContacts()
    {
    	// Get the base URI for People table in Contacts content provider.
    	// ie. content://contacts/people/
    	Uri mContacts = People.CONTENT_URI;  
    	       
    	// Best way to retrieve a query; returns a managed query. 
    	Cursor cursor = managedQuery( mContacts,
    	                        null, // Which columns to return. 
    	                        null,       // selection
    	                        null,       // selection args
    	                        null);      // Order-by clause.
    	int nColumns = cursor.getColumnCount();
    	String[] columnNames = cursor.getColumnNames();
    	while (cursor.moveToNext()) {
    		ContactDetails cd = new ContactDetails();
    		for (int i = 0; i < nColumns; i++) {
    			String value = cursor.getString(i);
    			if (value != null && value.length() > 1 && !columnNames[i].startsWith("_")) {
    				cd.pairs.add(new ContactDetails.Pair(columnNames[i], value));
    			}
    		}
    		index.add(cd);
    	}
    }
}