import org.garret.perst.*;

import java.util.*;

public class TestDerivedIndex { 
    static class BaseRecord extends Persistent { 
        @Indexable
        int key;
    }

    static class DerivedRecord extends BaseRecord { 
        int value;
    }

    static public void main(String[] args) {    
        Storage storage = StorageFactory.getInstance().createStorage();
        storage.open("testdbi.dbs");
        Database db = new Database(storage);
        DerivedRecord dr = new DerivedRecord();
        dr.key = 1;
        dr.value = 2;
        db.addRecord(dr);

        BaseRecord br = new BaseRecord();
        br.key = 1;
        db.addRecord(br);

        Iterator<DerivedRecord> di = db.<DerivedRecord>select(DerivedRecord.class, "key=1 and value=2");
        while (di.hasNext()) { 
            DerivedRecord rec = di.next();
            System.out.println("type=" + rec.getClass() + ", key=" + rec.key + ", value=" + rec.value);
        }
        Iterator<BaseRecord> bi = db.<BaseRecord>select(BaseRecord.class, "key=1");
        while (bi.hasNext()) { 
            BaseRecord rec = bi.next();
            System.out.println("type=" + rec.getClass() + ", key=" + rec.key);
        }
        storage.close();
    }
}