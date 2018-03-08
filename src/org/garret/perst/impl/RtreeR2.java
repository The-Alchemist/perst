package org.garret.perst.impl;

import org.garret.perst.*;
import java.util.*;

public class RtreeR2<T> extends PersistentCollection<T> implements SpatialIndexR2<T> {
    private int           height;
    private int           n;
    private RtreeR2Page   root;
    private transient int updateCounter;

    RtreeR2() {}
    RtreeR2(Storage storage) {
        super(storage);
    }

    public void put(RectangleR2 r, T obj) {
        Storage db = getStorage();
        if (root == null) { 
            root = new RtreeR2Page(db, obj, r);
            height = 1;
        } else { 
            RtreeR2Page p = root.insert(db, r, obj, height); 
            if (p != null) {
                root = new RtreeR2Page(db, root, p);
                height += 1;
            }
        }
        n += 1;
        updateCounter += 1;
        modify();
    }
    
    public int size() { 
        return n;
    }

    public void print() { 
        if (root != null) { 
            root.print(0, height);
        }
    }

    public void remove(RectangleR2 r, T obj) {
        if (root == null) { 
            throw new StorageError(StorageError.KEY_NOT_FOUND);
        }
        ArrayList reinsertList = new ArrayList();
        int reinsertLevel = root.remove(r, obj, height, reinsertList);
        if (reinsertLevel < 0) { 
             throw new StorageError(StorageError.KEY_NOT_FOUND);
        }        
        for (int i = reinsertList.size(); --i >= 0;) {
            RtreeR2Page p = (RtreeR2Page)reinsertList.get(i);
            for (int j = 0, n = p.n; j < n; j++) { 
                RtreeR2Page q = root.insert(getStorage(), p.b[j], p.branch.get(j), height - reinsertLevel); 
                if (q != null) { 
                    // root splitted
                    root = new RtreeR2Page(getStorage(), root, q);
                    height += 1;
                }
            }
            reinsertLevel -= 1;
            p.deallocate();
        }
        if (root.n == 1 && height > 1) { 
            RtreeR2Page newRoot = (RtreeR2Page)root.branch.get(0);
            root.deallocate();
            root = newRoot;
            height -= 1;
        }
        n -= 1;
        updateCounter += 1;
        modify();
    }
    
    public Object[] get(RectangleR2 r) {
        ArrayList result = new ArrayList();
        if (root != null) { 
            root.find(r, result, height);
        }
        return result.toArray();
    }

    public ArrayList<T> getList(RectangleR2 r) { 
        ArrayList<T> result = new ArrayList<T>();
        if (root != null) { 
            root.find(r, result, height);
        }
        return result;
    }

    public RectangleR2 getWrappingRectangle() {
        if (root != null) { 
            return root.cover();
        }
        return null;
    }

    public void clear() {
        if (root != null) { 
            root.purge(height);
            root = null;
        }
        height = 0;
        n = 0;
        updateCounter += 1;
        modify();
    }

    public void deallocate() {
        clear();
        super.deallocate();
    }

    public Object[] toArray() {
        return get(getWrappingRectangle());
    }

    public <E> E[] toArray(E[] arr) {
        return getList(getWrappingRectangle()).toArray(arr);
    }
    
    class RtreeIterator<E> extends IterableIterator<E> implements PersistentIterator {
        RtreeIterator(RectangleR2 r) { 
            counter = updateCounter;
            if (height == 0) { 
                return;
            }
            this.r = r;            
            pageStack = new RtreeR2Page[height];
            posStack = new int[height];

            if (!gotoFirstItem(0, root)) { 
                pageStack = null;
                posStack = null;
            }
        }

        public boolean hasNext() {
            if (counter != updateCounter) { 
                throw new ConcurrentModificationException();
            }
            return pageStack != null;
        }

        protected Object current(int sp) { 
            return pageStack[sp].branch.get(posStack[sp]);
        }

        public E next() {
            if (!hasNext()) { 
                throw new NoSuchElementException();
            }
            E curr = (E)current(height-1);
            if (!gotoNextItem(height-1)) { 
                pageStack = null;
                posStack = null;
            }
            return curr;
        }
 
        public int nextOid() {
            if (!hasNext()) { 
                return 0;
            }
            int oid = getStorage().getOid(pageStack[height-1].branch.getRaw(posStack[height-1]));
            if (!gotoNextItem(height-1)) { 
                pageStack = null;
                posStack = null;
            }
            return oid;
        }
        
        private boolean gotoFirstItem(int sp, RtreeR2Page pg) { 
            for (int i = 0, n = pg.n; i < n; i++) { 
                if (r.intersects(pg.b[i])) { 
                    if (sp+1 == height || gotoFirstItem(sp+1, (RtreeR2Page)pg.branch.get(i))) { 
                        pageStack[sp] = pg;
                        posStack[sp] = i;
                        return true;
                    }
                }
            }
            return false;
        }
              
 
        private boolean gotoNextItem(int sp) {
            RtreeR2Page pg = pageStack[sp];
            for (int i = posStack[sp], n = pg.n; ++i < n;) { 
                if (r.intersects(pg.b[i])) { 
                    if (sp+1 == height || gotoFirstItem(sp+1, (RtreeR2Page)pg.branch.get(i))) { 
                        pageStack[sp] = pg;
                        posStack[sp] = i;
                        return true;
                    }
                }
            }
            pageStack[sp] = null;
            return (sp > 0) ? gotoNextItem(sp-1) : false;
        }
              
        public void remove() { 
            throw new UnsupportedOperationException();
        }

        RtreeR2Page[] pageStack;
        int[]         posStack;
        int           counter;
        RectangleR2   r;
    }
    
    static class RtreeEntry<T> implements Map.Entry<RectangleR2,T> {
        RtreeR2Page pg;
        int         pos;

	public RectangleR2 getKey() {
	    return pg.b[pos];
	}

	public T getValue() {
	    return (T)pg.branch.get(pos);
	}

  	public T setValue(T value) {
            throw new UnsupportedOperationException();
        }

        RtreeEntry(RtreeR2Page pg, int pos) { 
            this.pg = pg;
            this.pos = pos;
        }
    }
        
    class RtreeEntryIterator extends RtreeIterator<Map.Entry<RectangleR2,T>> {
        RtreeEntryIterator(RectangleR2 r) { 
            super(r);
        }
        
        protected Object current(int sp) { 
            return new RtreeEntry(pageStack[sp], posStack[sp]);
        }
    }

    public Iterator<T> iterator() {
        return iterator(getWrappingRectangle());
    }

    public IterableIterator<Map.Entry<RectangleR2,T>> entryIterator() {
        return entryIterator(getWrappingRectangle());
    }

    public IterableIterator<T> iterator(RectangleR2 r) { 
        return new RtreeIterator<T>(r);
    }

    public IterableIterator<Map.Entry<RectangleR2,T>> entryIterator(RectangleR2 r) { 
        return new RtreeEntryIterator(r);
    }

    static class Neighbor { 
        Object   child;
        Neighbor next;
        int      level;
        double   distance;

        Neighbor(Object child, double distance, int level) { 
            this.child = child;
            this.distance = distance;
            this.level = level;
        }
    }

    class NeighborIterator<E> extends IterableIterator<E> implements PersistentIterator 
    {
        Neighbor list;
        int counter;
        double x;
        double y;
        Storage storage;

        NeighborIterator(double x, double y) { 
            this.x = x;
            this.y = y;
            counter = updateCounter;
            if (height == 0) { 
                return;
            }
            list = new Neighbor(root, root.cover().distance(x, y), height);
            storage = getStorage();
        }

        void insert(Neighbor node) { 
            Neighbor prev = null, next = list;
            double distance = node.distance;
            while (next != null && next.distance < distance) { 
                prev = next;
                next = prev.next;
            }
            node.next = next;
            if (prev == null) { 
                list = node;
            } else { 
                prev.next = node;
            }
        }

        public boolean hasNext() { 
            if (counter != updateCounter) { 
                throw new ConcurrentModificationException();
            }
            while (true) { 
                Neighbor neighbor = list;
                if (neighbor == null) { 
                    return false;
                }
                if (neighbor.level == 0) { 
                    return true;
                }
                list = neighbor.next;
                RtreeR2Page pg = (RtreeR2Page)(neighbor.child instanceof RtreeR2Page ? neighbor.child : storage.getObjectByOID(storage.getOid(neighbor.child)));
                for (int i = 0, n = pg.n; i < n; i++) { 
                    insert(new Neighbor(pg.branch.getRaw(i), pg.b[i].distance(x, y), neighbor.level-1));
                }
            }
        }

        public E next() {
            if (!hasNext()) { 
                throw new NoSuchElementException();
            }
            Neighbor neighbor = list;
            list = neighbor.next;
            Assert.that(neighbor.level == 0);
            return (E)(neighbor.child instanceof PersistentStub ? storage.getObjectByOID(storage.getOid(neighbor.child)) : neighbor.child);
        }

        public int nextOid() {
            if (!hasNext()) { 
                return 0;
            }
            Neighbor neighbor = list;
            list = neighbor.next;
            Assert.that(neighbor.level == 0);
            return storage.getOid(neighbor.child);
        }

        public void remove() { 
            throw new UnsupportedOperationException();
        }
    }
        
    public IterableIterator<T> neighborIterator(double x, double y) { 
        return new NeighborIterator(x, y);
    }
}
    

