import org.garret.perst.*;

import java.io.*;

public class TestPatricia { 
    static int pagePoolSize = 32*1024*1024;

    static void searchPrefix(PatriciaTrie trie, String prefix)
    {
        PersistentString match = (PersistentString)trie.findExactMatch(PatriciaTrieKey.from8bitString(prefix));
        if (match == null) { 
            System.out.println("Key '" + prefix + "' not found");
        } else { 
            System.out.println(prefix + "->" + match);
        }
    }


    public static void main(String args[]) throws IOException { 
        Storage db = StorageFactory.getInstance().createStorage();        
        db.open("testptree.dbs", pagePoolSize);
        PatriciaTrie root = (PatriciaTrie)db.getRoot();
        if (root == null) { 
            root = db.createPatriciaTrie();
            db.setRoot(root);
            root.add(PatriciaTrieKey.from8bitString("724885"), new PersistentString("ATT"));
            root.add(PatriciaTrieKey.from8bitString("72488547"), new PersistentString("BCC"));
        }
        searchPrefix(root, "724885");
        searchPrefix(root, "7248851");
        searchPrefix(root, "7248852");
        searchPrefix(root, "7248853");
        searchPrefix(root, "7248854");
        searchPrefix(root, "7248855");
        searchPrefix(root, "7248856");
        searchPrefix(root, "7248857");
        searchPrefix(root, "7248858");
        searchPrefix(root, "7248859");
        searchPrefix(root, "7248860");
        searchPrefix(root, "72488553");
        searchPrefix(root, "72488563");
        db.close();
    }
}
            