import org.garret.perst.*;
import java.io.File;


public class TestRollback 
{ 
    static class Record1 extends Persistent { 
        int count;
    }

    static class Record2 { 
        int count;
    }

    public static void main(String[] args) throws Exception
    {         
        Storage db = StorageFactory.getInstance().createStorage();
        long pagePoolSize = 32 * 1024 * 1024; 
        db.setProperty("perst.reload.objects.on.rollback", Boolean.TRUE);
        new File("testrollback.dbs").delete();
        db.open("testrollback.dbs", pagePoolSize);
        IPersistentSet root = (IPersistentSet)db.getRoot();
        if (root == null) {
            root = db.createSet();
            db.setRoot(root);
        }
        Record1 t1 = new Record1();
        t1.count = 1;
        root.add(t1);
        Record1 t2 = new Record1();
        t2.count = 2;
        root.add(t2);
        Record2 t3 = new Record2();
        t3.count = 3;
        root.add(t3);
        db.commit();

        t1.count += 100;
        t1.modify();
        t2.count += 100;
        t2.store();
        t3.count += 100;
        db.modify(t3);
        Record1 t4 = new Record1(); 
        t4.count = 4;
        root.add(t4);
        Record2 t5 = new Record2(); 
        t5.count = 5;
        root.add(t5);
        db.rollback();
        Assert.that(t1.count == 1);
        Assert.that(t2.count == 2);
        Assert.that(t3.count == 3);
    }
}