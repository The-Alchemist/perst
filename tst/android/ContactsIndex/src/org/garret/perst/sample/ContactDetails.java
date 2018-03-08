package org.garret.perst.sample;

import org.garret.perst.*;
import org.garret.perst.fulltext.*;

import java.util.Vector;
import java.io.Reader;
import java.io.StringReader;

public class ContactDetails extends Persistent implements FullTextSearchable {
	static class Pair implements IValue {
		String label;
		String value;

		Pair(String label, String value) {
			this.label = label;
			this.value = value;
		}
		
		private Pair() {}
	}

	Vector<Pair> pairs = new Vector<Pair>(0);

	public Reader getText() {
		StringBuffer buf = new StringBuffer();
		int nPairs = pairs.size();
		for (int i = 0; i < nPairs; i++) {
			Pair pair = (Pair) pairs.elementAt(i);
			buf.append(pair.value);
			buf.append('\n');
		}
		return new StringReader(buf.toString());
	}

	public String getLanguage() {
		return ContactsIndex.LANGUAGE;
	}

	public String buildSnippet(String query, FullTextSearchHelper helper) {
		String best = "";
		boolean exact = false;
		query = query.toLowerCase();
		int nPairs = pairs.size();
		for (int i = 0; i < nPairs; i++) {
			Pair pair = (Pair) pairs.elementAt(i);
			String snippet = pair.value;
			int pos = snippet.toLowerCase().indexOf(query);
			if (pos >= 0) {
				if ((pos == 0 || !helper.isWordChar(snippet.charAt(pos - 1)))
						&& (pos + query.length() == snippet.length() 
							|| !helper.isWordChar(snippet.charAt(pos + query.length())))) {
					if (!exact) {
						exact = true;
						best = snippet;
					} else if (snippet.length() > best.length()) {
						best = snippet;
					}
				} else if (!exact && snippet.length() > best.length()) {
					best = snippet;
				}
			}
		}
		if (best.length() == 0 && nPairs > 0) {
			Pair pair = (Pair) pairs.elementAt(0);
			best = pair.value;
		}
		return best;
	}
}
