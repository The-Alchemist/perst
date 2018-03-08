import org.garret.perst.*;

import java.util.*;

class DynamicClass extends PersistentResource 
{ 
    String name;
    IPersistentSet<DynamicObject> instances;
    Index<Index<DynamicObject>> fieldIndex;

    DynamicClass() {}

    public DynamicClass(Storage db, String name) { 
        super(db);
        this.name = name;
        instances = db.<DynamicObject>createSet();
        fieldIndex = db.<Index<DynamicObject>>createIndex(String.class, true);
    }

    public void deallocate() {
        instances.deallocate();
        fieldIndex.deallocate();
        super.deallocate();
    }
}
   
class DynamicObject extends SmallMap<String,Object> 
{
    DynamicClass cls;    

    public void deallocate() {
        for (Map.Entry<String,Object> field : entrySet()) {
            String fieldName = field.getKey();
            Object fieldValue = field.getValue();
            Index<DynamicObject> index = cls.fieldIndex.get(fieldName);
            index.remove(fieldValue, this);
            if (index.size() == 0) { 
                cls.fieldIndex.remove(fieldName);
            }
            cls.instances.remove(this);
        }
        super.deallocate();
    }

}

class ClassDictionary extends PersistentResource 
{
    FieldIndex<DynamicClass> classIndex;

    public ClassDictionary(Storage db) {
        super(db);
        classIndex = db.<DynamicClass>createFieldIndex(DynamicClass.class, "name", true);
    } 

    ClassDictionary() {}
}
    
public class TestDynamicObjects 
{ 
    final static int nClasses = 10;
    final static int nFields  = 100;
    final static int maxObjectFields  = 10;
    final static int nObjects = 100000;

    static int pagePoolSize = 256*1024*1024;
    static long rnd;

    static void resetRand() { 
        rnd = 1999;
    }

    static int nextRandInt(int mod) {
        rnd = (3141592621L*rnd + 2718281829L) % 1000000007L;
        return (int)(rnd % mod);
    }

    static long nextRandLong() {
        return rnd = (3141592621L*rnd + 2718281829L) % 1000000007L;
    }

    static public void main(String[] args) {    
        int i, j;        
        Storage db = StorageFactory.getInstance().createStorage();
        db.open("testdynobj.dbs", pagePoolSize);

        boolean populate = args.length > 0 && args[0].equals("populate");
        ClassDictionary dictionary = (ClassDictionary)db.getRoot();
        if (dictionary == null) { 
            dictionary = new ClassDictionary(db);
            db.setRoot(dictionary);
        }
        long start = System.currentTimeMillis();
        resetRand();
        if (dictionary.classIndex.size() == 0) { 
            for (i = 0; i < nClasses; i++) { 
                DynamicClass cls = new DynamicClass(db, "Class" + i);
                dictionary.classIndex.add(cls);
            }
            for (i = 0; i < nObjects; i++) { 
                Record rec = new Record();
                String className = "Class" + nextRandInt(nClasses);
                DynamicObject obj = new DynamicObject();
                obj.cls = dictionary.classIndex.get(className);
                int nObjFields = nextRandInt(maxObjectFields) + 1;
                int fieldNo = nextRandInt(nFields);
                for (j = 0; j < nObjFields; j++) { 
                    String fieldName = "Field" + (fieldNo++ % nFields);
                    long intValue = nextRandLong();
                    Object fieldValue = (nextRandInt(1) == 0) ? intValue : Long.toString(intValue);
                    obj.cls.instances.add(obj);
                    Index<DynamicObject> index = obj.cls.fieldIndex.get(fieldName);
                    if (index == null) { 
                        index = db.<DynamicObject>createIndex(fieldValue instanceof String ? String.class : long.class, false);
                        obj.cls.fieldIndex.put(fieldName, index);
                    }
                    index.put(fieldValue, obj);
                    obj.put(fieldName, fieldValue);
                }
            }
            db.commit();
            System.out.println("Elapsed time for inserting " + nObjects + " objects: " 
                               + (System.currentTimeMillis() - start) + " milliseconds");
        }
        resetRand();
        start = System.currentTimeMillis();

        for (i = 0; i < nObjects; i++) { 
            String className = "Class" + nextRandInt(nClasses);
            DynamicClass cls = dictionary.classIndex.get(className);
            int nObjFields = nextRandInt(maxObjectFields) + 1;
            HashSet<DynamicObject> objects = null;
            int fieldNo = nextRandInt(nFields);
            for (j = 0; j < nObjFields; j++) { 
                String fieldName = "Field" + (fieldNo++ % nFields);
                long intValue = nextRandLong();
                Object fieldValue = (nextRandInt(1) == 0) ? intValue : Long.toString(intValue);
                Index<DynamicObject> index = cls.fieldIndex.get(fieldName);
                ArrayList<DynamicObject> occurrences = index.getList(fieldValue, fieldValue);
                for (DynamicObject obj : occurrences) {
                    Object value = obj.get(fieldName);
                    Assert.that(obj.get(fieldName).equals(fieldValue));
                    Assert.that(obj.cls == cls);
                }
                if (objects == null) { 
                    objects = new HashSet<DynamicObject>(occurrences);
                } else {
                    objects.retainAll(occurrences);
                }
            }
            Assert.that(objects.size() > 0);
        }
        System.out.println("Elapsed time for performing " + nObjects + " object searches: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");

        start = System.currentTimeMillis();
        for (i = 0, j = 0; i < nClasses; i++) { 
            String className = "Class" + i;
            DynamicClass cls = dictionary.classIndex.get(className);
            for (DynamicObject obj : cls.instances) {
                Assert.that(obj.cls == cls);
                j += 1;
            }
        }
        Assert.that(j == nObjects);
        System.out.println("Elapsed time for traversing " + nObjects + ": " 
                           + (System.currentTimeMillis() - start) + " milliseconds");

        if (!populate) { 
            start = System.currentTimeMillis();
            Iterator<DynamicClass> classes = dictionary.classIndex.iterator();
            for (i = 0, j = 0; classes.hasNext(); i++) { 
                DynamicClass cls = classes.next();
                Iterator<DynamicObject> instances = cls.instances.iterator();
                while (instances.hasNext()) { 
                    DynamicObject obj = instances.next();
                    instances.remove();
                    obj.deallocate();
                    j += 1;
                }
                classes.remove();
                cls.deallocate();
            }
            Assert.that(i == nClasses);
            Assert.that(j == nObjects);
            System.out.println("Elapsed time for deleting " + nObjects + " objects: " 
                               + (System.currentTimeMillis() - start) + " milliseconds");
        }
        db.close();
    }
}
