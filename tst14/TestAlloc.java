import org.garret.perst.*;

import java.io.*;
import java.util.*;

public class TestAlloc { 
    public static void main(String[] args) throws Exception { 
        Storage db = StorageFactory.getInstance().createStorage();
        db.open("@testalloc.mfd");
        Index root = (Index)db.getRoot();
        byte[] buf = new byte[1024];
        int rc;
        File dir = new File(".");
        String[] files = dir.list();
        if (root == null || root.size() == 0) { 
            if (root == null) { 
                root = db.createIndex(String.class, true);
                db.setRoot(root);
                db.registerCustomAllocator(Blob.class, db.createBitmapAllocator(1024, 0x1000000000000000L, 0x100000, Long.MAX_VALUE));
            }
            for (int i = 0; i < files.length; i++) { 
                if (files[i].endsWith(".java")) {
                    FileInputStream in = new FileInputStream(files[i]);
                    Blob blob = db.createBlob();                    
                    OutputStream out = blob.getOutputStream(false);
                    while ((rc = in.read(buf)) > 0) { 
                        out.write(buf, 0, rc);
                    }
                    root.put(files[i], blob);   
                    in.close();
                    out.close();
                }
            }
            System.out.println("Database is initialized");
        } else { 
            for (int i = 0; i < files.length; i++) { 
                if (files[i].endsWith(".java")) {
                    byte[] buf2 = new byte[1024];
                    Blob blob = (Blob)root.get(files[i]);
                    if (blob == null) {
                        System.err.println("File " + files[i] + " not found in database");
                        continue;
                    }
                    InputStream bin = blob.getInputStream();
                    InputStream fin = new FileInputStream(files[i]);
                    while ((rc = fin.read(buf)) > 0) { 
                        int rc2 = bin.read(buf2);
                        if (rc != rc2) {
                            System.err.println("Different file size: " + rc + " .vs. " + rc2);
                            break;
                        }
                        while (--rc >= 0 && buf[rc] == buf2[rc]);
                        if (rc >= 0) { 
                            System.err.println("Content of the files is different: " + buf[rc] + " .vs. " + buf2[rc2]);
                            break;
                        }
                    }
                    bin.close();
                    fin.close();
                }            
            }            
            System.out.println("Verification completed");
            Iterator iterator = root.iterator(null, null, Index.ASCENT_ORDER);
            while (iterator.hasNext()) { 
                Blob blob = (Blob)iterator.next();
                iterator.remove();
                blob.deallocate();
            }
            //root.put("dummy", db.createBlob());
            System.out.println("Cleanup completed");                
        }
        db.close();
    }
}

