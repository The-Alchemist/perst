import org.garret.perst.*;
import java.util.Iterator;
import java.io.File;

class BugReport extends Version {
    public static final int SEVERITY_LOW      = 1;
    public static final int SEVERITY_NORMAL   = 2;
    public static final int SEVERITY_CRITICAL = 3;
    public static final int SEVERITY_URGENT   = 4;

    public static final int STATUS_SUBMITTED  = 0;
    public static final int STATUS_OPEN       = 1;
    public static final int STATUS_CLOSED     = 2;
    public static final int STATUS_REJECTED   = 3;

    String         description;
    int            severity;
    int            status;
    VersionHistory product;

    public String getDescription() { 
        return description;
    }
    
    public int getSeverity() { 
        return severity;
    }

    public int getStatus() { 
        return status;
    }

    public VersionHistory getProduct() { 
        return product;
    }

    public BugReport(Storage storage, String description, int severity, VersionHistory product) { 
        super(storage);
        this.description = description;
        this.severity = severity;
        this.product = product;
        status = STATUS_SUBMITTED;
    }
}

class Release extends Version { 
    String name;
    String releaseNotes;

    public String getName() { 
        return name;
    }

    public String getReleaseNotes() { 
        return releaseNotes;
    }

    public Release(Storage storage, String name) { 
        super(storage);
        this.name = name;
        this.releaseNotes = "Initial release";
    }
}

class BugTrackingSystem extends Persistent { 
    Index bugReports;
    Index products;

    public BugTrackingSystem(Storage storage) {
        super(storage);
        bugReports = storage.createIndex(String.class, true);
        products = storage.createIndex(String.class, true);
    }

    public boolean addProduct(String name) { 
        return products.put(name, new VersionHistory(new Release(getStorage(), name)));
    }

    public boolean addRelease(String name, String releaseNotes) { 
        VersionHistory vh = (VersionHistory)products.get(name);
        if (vh != null) { 
            Release release = (Release)vh.checkout();
            release.releaseNotes = releaseNotes;
            release.checkin();
            return true;
        }
        return false;
    }

    public boolean addBugReport(String id, String description, int severity, String productName) { 
        VersionHistory product = (VersionHistory)products.get(productName);
        if (product == null) { 
            return false;
        }
        return bugReports.put(id, new VersionHistory(new BugReport(getStorage(), description, severity, product)));
    }

    public boolean changeBugReport(String id, int status) { 
        VersionHistory vh = (VersionHistory)bugReports.get(id);
        if (vh == null) { 
            return false;
        }
        BugReport cr = (BugReport)vh.checkout();
        cr.status = status;
        cr.checkin();
        return true;
    }

    public Iterator productIterator() { 
        return products.iterator();
    }

    public Iterator bugReportIterator() { 
        return bugReports.iterator();
    }
}

public class TestVersion 
{ 
    public static final int N_PRODUCTS = 10;
    public static final int N_BUG_REPORTS = 10000;
    public static final long DELAY = 1000;

    public static void main(String args[]) 
    { 
        Storage db = StorageFactory.getInstance().createStorage();
        db.open("bugdb.dbs");
        BugTrackingSystem bts = (BugTrackingSystem)db.getRoot();
        if (bts == null) { 
            bts = new BugTrackingSystem(db);
            db.setRoot(bts);
        }
        for (int i = 0; i < N_PRODUCTS; i++) { 
            Assert.that(bts.addProduct("Product" + (i + 1)));
        }
        for (int i = 0; i < N_BUG_REPORTS; i++) { 
            Assert.that(bts.addBugReport("Bug" + i, "Something is definitely wrong", BugReport.SEVERITY_URGENT,
                                         "Product" + (i % N_PRODUCTS + 1)));
        }
        for (int i = 0; i < N_BUG_REPORTS; i++) { 
            Assert.that(bts.changeBugReport("Bug" + i, BugReport.STATUS_OPEN));
        }
        for (int i = 0; i < N_PRODUCTS; i++) { 
            Assert.that(bts.addRelease("Product" + (i + 1), "All bugs are fixed"));
        }
        for (int i = 0; i < N_BUG_REPORTS; i++) { 
            Assert.that(bts.changeBugReport("Bug" + i, BugReport.STATUS_CLOSED));
        }
        Iterator iterator = bts.bugReportIterator();
        int n = 0;
        while (iterator.hasNext()) { 
            VersionHistory vh = (VersionHistory)iterator.next();
            BugReport cr1 = (BugReport)vh.getRoot();
            BugReport cr2 = (BugReport)vh.getCurrent();
            Assert.that(cr1.status == BugReport.STATUS_SUBMITTED);
            Assert.that(cr2.status == BugReport.STATUS_CLOSED);
            vh = cr2.getProduct();
            Release r1 = (Release)vh.getLatestBefore(cr1.getDate());
            Assert.that(r1.releaseNotes.equals("Initial release"));
            Release r2 = (Release)vh.getCurrent();
            Assert.that(r2.releaseNotes.equals("All bugs are fixed"));
            n += 1;
        }
        Assert.that(n == N_BUG_REPORTS);
        db.close();
        File dbFile = new File("bugdb.dbs");
        dbFile.delete();
    }
}       
        
