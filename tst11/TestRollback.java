import org.garret.perst.*;
import java.io.File;


public class TestRollback 
{ 
    public static class Record extends Persistent 
    { 
        int count;

        public void writeObject(IOutputStream out) {
            out.writeInt(count);
        }
        public void readObject(IInputStream in) {
            count = in.readInt();
        }
    }

    public static void main(String[] args) throws Exception
    {         
        Storage db = StorageFactory.getInstance().createStorage();
        int  pagePoolSize = 32 * 1024 * 1024; 
        db.setProperty("perst.reload.objects.on.rollback", Boolean.TRUE);
        new File("testrollback.dbs").delete();
        db.open("testrollback.dbs", pagePoolSize);
        IPersistentSet root = (IPersistentSet)db.getRoot();
        if (root == null) {
            root = db.createSet();
            db.setRoot(root);
        }
        Record t1 = new Record();
        t1.count = 1;
        root.add(t1);
        Record t2 = new Record();
        t2.count = 2;
        root.add(t2);
        db.commit();

        t1.count += 100;
        t1.modify();
        t2.count += 100;
        t2.store();
        Record t3 = new Record(); 
        t3.count = 3;
        root.add(t3);
        db.rollback();
        Assert.that(t1.count == 1);
        Assert.that(t2.count == 2);
    }
}