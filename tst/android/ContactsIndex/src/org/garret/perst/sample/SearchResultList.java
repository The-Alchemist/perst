package org.garret.perst.sample;

import android.app.ListActivity;
import android.content.Intent;
import android.os.Bundle;
import android.view.View;
import android.widget.ArrayAdapter;
import android.widget.ListView;

import org.garret.perst.fulltext.*;


public class SearchResultList extends ListActivity {
	FullTextSearchResult result;
 
    /** Called when the activity is first created. */
    @Override
    public void onCreate(Bundle savedInstanceState) {
     	ContactsIndex main = ContactsIndex.instance;
    	super.onCreate(savedInstanceState);
    	Intent intent = getIntent();
    	String query = intent.getStringExtra("query");
        result = main.search(query);
        String[] snippets = new String[result.hits.length];
    	for (int i = 0; i < snippets.length; i++) { 
            ContactDetails cd = (ContactDetails)result.hits[i].getDocument();
            snippets[i] = cd.buildSnippet(query, main.index.getHelper());
        }
    	setTitle("Found " + result.estimation + " results");
        setListAdapter(new ArrayAdapter<String>(this,
                android.R.layout.simple_list_item_1, snippets));
    }
    
    @Override
    protected void onListItemClick(ListView l, View v, int position, long id) 
    {    
    	Intent intent = new Intent(Intent.ACTION_VIEW);
    	intent.setClass(this, SearchResultForm.class);
    	intent.putExtra("rank", result.hits[position].rank);
    	intent.putExtra("oid", result.hits[position].oid);
    	startActivity(intent);
    }
}
