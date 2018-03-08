import org.garret.perst.*;

import java.util.*;

public class TestJSQLContains 
{ 
    static class A { 
        String parent;
        Link<B> b;
    };
    
    static class B {
        String child;
    }

    static final int countA = 100;
    static final int countB = 100;

    static public void main(String[] args) {    
        Storage storage = StorageFactory.getInstance().createStorage(); 
        storage.open("testjsqlcontains.dbs");
        Database db = new Database(storage);

        long start = System.currentTimeMillis();
        for (int i = 0; i < countA; i++) {
            A a = new A();
            a.parent = "A." + i;
            a.b = storage.<B>createLink(countB);
            a.b.setSize(countB);
            for (int j = 0; j < countB; j++) {             
                B b = new B();
                b.child = "B." + i + "." + j;
                db.addRecord(b);
                a.b.set(j, b);
            }
            db.addRecord(a);
        }
        db.commitTransaction();
        long end = System.currentTimeMillis();
        System.out.println("Elapsed time for inserting " + countA*countB + " records: " 
                           + (end - start) + " milliseconds");
            
        int i = 0;
        for (A a : db.<A>select(A.class, "contains b with child like '%.0'")) { 
            Assert.that(a.parent.equals("A." + i));
            i += 1;
        }
        Assert.that(i == countA);
        end = System.currentTimeMillis();
        System.out.println("Elapsed time for query execution: " + (end - start) + " milliseconds");
        
        db.dropTable(A.class);
        db.dropTable(B.class);
        
        storage.close();
    }
}
