package org.garret.bench;

import java.io.PrintStream;

import android.database.sqlite.*;
import android.database.*;

public class SqlLiteTest extends Test {
	final static int nRecords = 10000;
	final static int pagePoolSize = 2 * 1024 * 1024;
    
	String databaseName;
    PrintStream out;
    
    SqlLiteTest(String databaseName, PrintStream out) {
    	this.databaseName = databaseName;
    	this.out = out;
    }

    public String getName() { 
    	return "SqLite";
    }

    
    public void run()
    {
        SQLiteDatabase db = SQLiteDatabase.openOrCreateDatabase(databaseName, null);
		db.execSQL("create table TestIndex (i integer(8), s text)");
		db.execSQL("create index StrIndex on TestIndex (s)");
		db.execSQL("create index IntIndex on TestIndex (i)");
		SQLiteStatement stmt = db.compileStatement("insert into TestIndex (i,s) values (?,?)");
		long start = System.currentTimeMillis();
		long key = 1999;
		int i;
        db.execSQL("BEGIN");
		for (i = 0; i < nRecords; i++) {
			key = (3141592621L * key + 2718281829L) % 1000000007L;
			stmt.bindLong(1, key);
			stmt.bindString(2, Long.toString(key));
			stmt.execute();
		}
        db.execSQL("COMMIT");
		stmt.close();
		out.println("Elapsed time for inserting " + nRecords + " records: "
				+ (System.currentTimeMillis() - start) + " milliseconds");

		start = System.currentTimeMillis();
		key = 1999;
		for (i = 0; i < nRecords; i++) {
			key = (3141592621L * key + 2718281829L) % 1000000007L;
			String s = Long.toString(key);
			Cursor c1 = db.query("TestIndex", new String[]{"i", "s"}, "i=?", new String[]{s}, null, null, null);
			Cursor c2 = db.query("TestIndex", new String[]{"i", "s"}, "s=?", new String[]{s}, null, null, null);
			boolean found = c1.moveToFirst();
			assert(found);
			assert(c1.getLong(1) == key);
			assert(c1.getString(2).equals(s));
			assert(!c1.moveToNext());
			c1.close();
			
			found = c2.moveToFirst();
			assert(found);
			assert(c2.getLong(1) == key);
			assert(c2.getString(2).equals(s));
			assert(!c2.moveToNext());
			c2.close();
		}
		out.println("Elapsed time for performing " + nRecords * 2
				+ " index searches: " + (System.currentTimeMillis() - start)
				+ " milliseconds");

		start = System.currentTimeMillis();
		key = Long.MIN_VALUE;
		i = 0;
		Cursor c1 = db.query("TestIndex", new String[]{"i", "s"}, null, null, null, null, "i");
		while (c1.moveToNext()) {
			assert(c1.getLong(1) >= key);
			key = c1.getLong(1);
			i += 1;
		}
		c1.close();
		assert (i == nRecords);
		String s = "";
		i = 0;
		Cursor c2 = db.query("TestIndex", new String[]{"i", "s"}, null, null, null, null, "s");
		while (c2.moveToNext()) {
			assert(c2.getString(1).compareTo(s) >= 0);
			s = c2.getString(1);
			i += 1;
		}
		assert (i == nRecords);
		out.println("Elapsed time for iterating through " + (nRecords * 2)
				+ " records: " + (System.currentTimeMillis() - start)
				+ " milliseconds");

		start = System.currentTimeMillis();
		key = 1999;
		stmt = db.compileStatement("delete from TestIndex where i=?");
        db.execSQL("BEGIN");
		for (i = 0; i < nRecords; i++) {
			key = (3141592621L * key + 2718281829L) % 1000000007L;
			stmt.bindLong(1, key);
			stmt.execute();
		}
        db.execSQL("COMMIT");
		stmt.close();
		out.println("Elapsed time for deleting " + nRecords + " records: "
				+ (System.currentTimeMillis() - start) + " milliseconds");
		db.close();
		out.flush();
		done();
	}

}
