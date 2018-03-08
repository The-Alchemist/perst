import org.garret.perst.*;

import java.util.ArrayList;

class L1List { 
    L1List next;
    Object obj;
    Object root;

    L1List(Object value, Object tree, L1List list) { 
        obj = value;
        root = tree;
        next = list;
    }
};

class ListItem {
    int id;

    ListItem(int id) { 
        this.id = id;
    }
}

public class TestRaw extends Persistent { 
    L1List list;
    ArrayList<ListItem> array;
    Object  nil;

    static final int nListMembers = 100;
    static final int nArrayElements = 1000;


    public static void main(String[] args) { 
        Storage db = StorageFactory.getInstance().createStorage();
        db.open("testraw.dbs");
        TestRaw root = (TestRaw)db.getRoot();
        if (root == null) { 
            root = new TestRaw();
            db.setRoot(root);
            L1List list = null;
            for (int i = 0; i < nListMembers; i++) { 
                list = new L1List(new Integer(i), root, list);
            } 
            root.list = list;
            root.array = new ArrayList<ListItem>(nArrayElements);
            for (int i = 0; i < nArrayElements; i++) { 
                root.array.add(new ListItem(i));
            }
            root.store();
            System.out.println("Initialization of database completed");
        } 
        L1List list = root.list;
        for (int i = nListMembers; --i >= 0;) { 
            Assert.that(list.obj.equals(new Integer(i)));
            Assert.that(root == list.root);
            list = list.next;
        }
        for (int i = nArrayElements; --i >= 0;) { 
            Assert.that(root.array.get(i).id == i);
        }
        System.out.println("Database is OK");
        db.close();
    }
}

