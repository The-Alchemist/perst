package org.garret.perst.sample;

import android.app.ListActivity;
import android.content.Intent;
import android.os.Bundle;
import android.widget.SimpleAdapter;
import java.util.*;

public class SearchResultForm extends ListActivity 
{
	static final String LABEL = "Label";
    static final String VALUE = "Value";
 	
    /** Called when the activity is first created. */
    @Override
    public void onCreate(Bundle savedInstanceState) {
    	super.onCreate(savedInstanceState);
    	Intent intent = getIntent();
    	Bundle bundle = intent.getExtras();
    	float rank = bundle.getFloat("rank");
    	int oid = bundle.getInt("oid");
 		setTitle("Rank " + rank);
 		ContactDetails cd = (ContactDetails)ContactsIndex.instance.db.getObjectByOID(oid);
		Vector<ContactDetails.Pair> pairs = cd.pairs;
		int nPairs = pairs.size();
		ArrayList<Map<String, String>> list = new ArrayList<Map<String, String>>();
		for (int i = 0; i < nPairs; i++) {
			ContactDetails.Pair pair = (ContactDetails.Pair) pairs.get(i);
			Map<String, String> map = new java.util.HashMap<String, String>();
			map.put(LABEL, pair.label);
			map.put(VALUE, pair.value);
			list.add(map);
		}
		SimpleAdapter adapter = new SimpleAdapter(this, list,
				android.R.layout.simple_list_item_2, new String[] { LABEL,
						VALUE }, new int[] { android.R.id.text1,
						android.R.id.text2 });
		setListAdapter(adapter);
	}
}
