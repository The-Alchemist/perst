package org.garret.bench;

import android.app.Activity;
import android.os.Bundle;
import android.widget.*;
import android.view.*;
import java.io.*;

public class Benchmark extends Activity {
    
    static final private int PERST_ID = Menu.FIRST;
    static final private int SQLITE_ID = Menu.FIRST + 1;
    
    TextView tv;
    
    /**
     * Called when your activity's options menu needs to be created.
     */
    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        super.onCreateOptionsMenu(menu);

        // We are going to create two menus. Note that we assign them
        // unique integer IDs, labels from our string resources, and
        // given them shortcuts.
        menu.add(0, PERST_ID, 0, R.string.perst);
        menu.add(0, SQLITE_ID, 0, R.string.sqlite);

        return true;
    }

    /** Called when the activity is first created. */
    @Override
    public void onCreate(Bundle icicle) {
        super.onCreate(icicle);
        tv = new TextView(this);
        tv.setText(R.string.about);
        setContentView(tv);
    }
    /**
     * Called when a menu item is selected.
     */
    @Override
    public boolean onOptionsItemSelected(MenuItem item) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        String databasePath = "testindex.dbs";
        try { 
            // It is necessary to create file using Activity.openFileOutput method otherwise
            // java.io.RandomAccessFile will not able to open it (even in write mode). It seems to be a bug
            // which I expect to be fixed in next release of Android, but right now this two lines fix
            // this problem.
            this.openFileOutput(databasePath, 0).close();
            databasePath = getFileStreamPath(databasePath).getAbsolutePath();	
        } catch (IOException x) {}
        PrintStream ps = new PrintStream(out);
        Test test;
        switch (item.getItemId()) {
        case PERST_ID:
            test = new PerstTest(databasePath, ps);
            break;
        case SQLITE_ID:
            test = new SqlLiteTest(databasePath, ps);
            break;
        default:
            return super.onOptionsItemSelected(item);
        }
        new Thread(test).start();
        new Thread(new ProgressMonitor(test, tv, out)).start(); 
        return true;
   }

}