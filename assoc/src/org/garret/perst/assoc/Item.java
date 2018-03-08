package org.garret.perst.assoc;

import org.garret.perst.*;
import java.util.*;

/**
 * AssocDB item or object. Item is collection of some attributes (numeric or string)
 * and it is part of some relations (one-to-one, one-to-many, many-to-many). 
 * Each attribute has name (string). It is possible to associate with item  several values with the same name - 
 * something like array. Newly added values will be appended to the end of "array".
 * The same is true for link (relations) except that AssocDB can make a decision not to embed relation inside object and store it 
 * externally.
 * Item has no predefined attributes - you are free to add any attribute you like. For example if you need to maintain class
 * object objects, you can add "class" attribute.
 * Attribute names are not stored inside object - just their identifiers (integers). AssocDB class is responsible to
 * map IDs to names and visa versa.
 */
public class Item extends Persistent implements Comparable<Item>, Iterable<Map.Entry<String,Object>>
{
    protected int[] fieldIds;
    protected String[] stringFields;
    protected double[] numericFields;
    protected Link<Item> relations;

    protected transient AssocDB db;
    protected transient String[] fieldNames;

    public int compareTo(Item item) 
    { 
        int oid1 = getOid();
        int oid2 = item.getOid();
        return oid1 < oid2 ? -1 : oid1 == oid2 ? 0 : 1;
    }

    class ItemMapEntry implements Map.Entry<String,Object>
    {
        public String getKey() { 
            int id = pos < fieldIds.length ? fieldIds[pos] : ((Long)entry.getKey()).intValue();
            return db.id2name.get(id);
        }

        public Object getValue() { 
            if (pos < stringFields.length) { 
                return stringFields[pos];
            } else if (pos < stringFields.length + numericFields.length) {
                return numericFields[pos - stringFields.length];
            } else if (pos < fieldIds.length) { 
                return relations.get(pos - stringFields.length + numericFields.length);
            } else {
                return entry.getValue();
            }
        }

        public Object setValue(Object value) {
            throw new UnsupportedOperationException();
        }
        
        ItemMapEntry(int pos, Map.Entry<Object,Item> entry) { 
            this.pos = pos;
            this.entry = entry;
        }

        int pos;
        Map.Entry<Object,Item> entry;
    }

    class ItemIterator implements Iterator<Map.Entry<String,Object>>
    {
        public boolean hasNext() {
            return !eof;
        }

        public void remove() { 
            throw new UnsupportedOperationException();
        }

        public Map.Entry<String,Object> next() 
        {
            if (eof) { 
                throw new NoSuchElementException();
            }
            Map.Entry<String,Object> curr = new ItemMapEntry(i, entry);
            eof = moveNext();
            return curr;
        }

        final boolean moveNext() 
        {
            if (i+1 < fieldIds.length) { 
                i += 1;         
                return true;
            } else if (relations != null) { 
                if (iterator == null) { 
                    long oid = getOid();
                    iterator = db.root.relations.entryIterator(new Key(oid << 32, true), new Key((oid+1) << 32, false), Index.ASCENT_ORDER);
                }
                if (iterator.hasNext()) { 
                    entry = iterator.next();
                    return true;
                }
            }
            return false;
        }

        ItemIterator() { 
            i = -1;
            eof = moveNext();
        }

        int i;
        boolean eof;
        Map.Entry<Object,Item> entry;
        Iterator<Map.Entry<Object,Item>> iterator;
    }

    /**
     * Get iterator through all item attributes (inclusing relations)
     * @return iterator returning Map.Entry for each item association
     */
    public Iterator<Map.Entry<String,Object>> iterator() { 
        return new ItemIterator();
    }

    /**
     * Get list of all item links names
     * @return array of unique links in alphabet order
     */
    public String[] getAttributeNames() 
    { 
        if (fieldNames == null) { 
            int n = fieldIds.length; 
            int id = 0;
            ArrayList<String> list = new ArrayList<String>(n);
            for (int i = 0; i < n; i++) {
                if (fieldIds[i] != id) { 
                    id = fieldIds[i];
                    list.add(db.id2name.get(id));
                }
            }
            if (relations == null) { 
                long oid = getOid();
                for (Map.Entry<Object,Item> e : db.root.relations.entryIterator(new Key(oid << 32, true), new Key((oid+1) << 32, false), Index.ASCENT_ORDER))
                {
                    int nextId = (int)((Long)e.getKey()).longValue();
                    if (nextId != id) { 
                        id = nextId;
                        list.add(db.id2name.get(id));
                    }
                }
            }
            fieldNames = list.toArray(new String[list.size()]);
            Arrays.sort(fieldNames);
        }
        return fieldNames;
    }
    
    /**
     * Get value of attribute
     * @param name attribute name
     * @return  one of
     * <ul>
     * <li>object (String, Double or Item) if there is only one values associated with this name</li>
     * <li>array of String, double at Item if there are more than one values associated with this name</li>
     * <li>null if this item has not attribute with specified name</li>
     * </ul>
     */ 
    public Object getAttribute(String name)
    {
        Integer idWrapper = db.name2id.get(name);
        if (idWrapper == null) { 
            return null;
        }
        int id = idWrapper.intValue();
        return getAttribute(id);
    }

    protected Object getAttribute(int id) 
    { 
        int l = 0, n = stringFields.length, r = n;
        while (l < r) { 
            int m = (l + r) >>> 1;
            if (fieldIds[m] < id) { 
                l = m + 1;
            } else { 
                r = m;
            }
        }
        if (r == n || fieldIds[r] != id) { 
            l = n;
            n += numericFields.length;
            r = n;
            while (l < r) { 
                int m = (l + r) >>> 1;
                if (fieldIds[m] < id) { 
                    l = m + 1;
                } else { 
                    r = m;
                }
            }
            if (r == n || fieldIds[r] != id) { 
                return getRelation(id);
            }
        }
        for (r = l+1; r < n && fieldIds[r] == id; r++);
        if (r > l + 1) { 
            if (l < stringFields.length) { 
                String[] arr = new String[r-l];
                System.arraycopy(stringFields, l, arr, 0, r-l);
                return arr;
            } else { 
                double[] arr = new double[r-l];
                System.arraycopy(numericFields, l - stringFields.length, arr, 0, r-l);
                return arr;
            }
        } else { 
            return (l < stringFields.length) ? (Object)stringFields[l] : (Object)new Double(numericFields[l - stringFields.length]);
        }
    }
    
    /**
     * Returns value of string attribute
     * @param name attribute name
     * @return String value of attribute or null if the item has no attribute with such name.
     * If there are several values associated with this name, then first of them is returned
     */
    public String getString(String name)     
    { 
        Integer idWrapper = db.name2id.get(name);
        if (idWrapper == null) { 
            return null;
        }
        int id = idWrapper.intValue();
        int l = 0, n = stringFields.length, r = n;
        while (l < r) { 
            int m = (l + r) >>> 1;
            if (fieldIds[m] < id) { 
                l = m + 1;
            } else { 
                r = m;
            }
        }
        return (l == n || fieldIds[l] != id) ? null : stringFields[l];
    }

    /**
     * Returns value of numeric attribute
     * @param name attribute name
     * @return Double value of attribute or null if the item has no attribute with such name.
     * If there are several values associated with this name, then first of them is returned
     */
    public Double getNumber(String name) 
    { 
        Integer idWrapper = db.name2id.get(name);
        if (idWrapper == null) { 
            return null;
        }
        int id = idWrapper.intValue();
        int nStrings = stringFields.length;
        int l = nStrings, n = l + numericFields.length, r = n;
        while (l < r) { 
            int m = (l + r) >>> 1;
            if (fieldIds[m] < id) { 
                l = m + 1;
            } else { 
                r = m;
            }
        }
        return (l == n || fieldIds[l] != id) ? null : numericFields[l - nStrings];
    }

    /**
     * Get list of all item's attribute values (not including relation links)
     * @return array of name-value pairs associated with with item
     */
    public Pair[] getAttributes() 
    { 
        Pair[] pairs = new Pair[stringFields.length + numericFields.length];
        int i = 0, j;
        for (j = 0; j < stringFields.length; i++, j++) { 
            pairs[i] = new Pair(db.id2name.get(fieldIds[i]), stringFields[j]); 
        }
        for (j = 0; j < numericFields.length; i++, j++) { 
            pairs[i] = new Pair(db.id2name.get(fieldIds[i]), numericFields[j]); 
        }
        return pairs;
    }

    class SmallRelation extends IterableIterator<Item> implements PersistentIterator
    {
        public boolean hasNext() { 
            return l < r;
        }
        
        public Item next() { 
            return relations.get(l++);
        }
        
        public int nextOid() { 
            return l < r ? ((IPersistent)relations.getRaw(l++)).getOid() : 0;
        }

        SmallRelation(int from, int till) { 
            l = from;
            r = till;
        }
        int l, r;
    }
            
    public IterableIterator<Item> getRelation(String name)
    {
        Integer idWrapper = db.name2id.get(name);
        if (idWrapper == null) { 
            return null;
        }
        return getRelation(idWrapper.intValue());
    }

    protected IterableIterator<Item> getRelation(int id)
    {
        if (relations == null) { 
            long key = ((long)getOid() << 32) | id;
            return db.root.relations.iterator(new Key(key, true), new Key(key+1, false), Index.ASCENT_ORDER);
        }
        int first = stringFields.length + numericFields.length;
        int l = first, n = fieldIds.length, r = n;
        while (l < r) { 
            int m = (l + r) >>> 1;
            if (fieldIds[m] < id) { 
                l = m + 1;
            } else { 
                r = m;
            }
        }
        if (l == n || fieldIds[l] != id) { 
            return null;
        }
        for (r = l+1; r < fieldIds.length && fieldIds[r] == id; r++);
        return new SmallRelation(l - first, r - first);
    }

    public IterableIterator<Pair> getRelations() 
    {
        if (relations != null) { 
            return new IterableIterator<Pair>() 
            { 
                public boolean hasNext() { 
                    return i < relations.size();
                }
                
                public Pair next() { 
                    Pair pair = new Pair(db.id2name.get(fieldIds[i + stringFields.length + numericFields.length]), relations.get(i));
                    i += 1;
                    return pair;
                }
                private int i;
            };
        } else { 
            long oid = getOid();
            final IterableIterator<Map.Entry<Object,Item>> iterator 
                = db.root.relations.entryIterator(new Key(oid << 32, true), new Key((oid+1) << 32, false), Index.ASCENT_ORDER);
            return new IterableIterator<Pair>() 
            { 
                public boolean hasNext() { 
                    return iterator.hasNext();
                }
                
                public Pair next() { 
                    Map.Entry e = iterator.next();
                    return new Pair(db.id2name.get((int)((Long)e.getKey()).longValue()), e.getValue());
                }
            };
        }
    }            

    public void onLoad() 
    {
        db = ((AssocDB.Root)getStorage().getRoot()).db;
    }

    protected Item() {}

    protected Item(AssocDB db) 
    { 
        super(db.storage);
        this.db = db;
        fieldIds = new int[0];
        stringFields = new String[0];
        numericFields = new double[0];
        relations = db.storage.<Item>createLink(0);
    }
}
    