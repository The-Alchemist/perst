package org.garret.perst.impl;

import org.garret.perst.*;
import java.util.*;

public class RtreeRn<T> extends PersistentCollection<T> implements SpatialIndexRn<T> {
    private int           height;
    private int           n;
    private RtreeRnPage   root;
    private transient int updateCounter;

    RtreeRn() {}
    RtreeRn(Storage storage) {
        super(storage);
    }

    public void put(RectangleRn r, T obj) {
        Storage db = getStorage();
        if (root == null) { 
            root = new RtreeRnPage(db, obj, r);
            height = 1;
        } else { 
            RtreeRnPage p = root.insert(db, r, obj, height); 
            if (p != null) {
                root = new RtreeRnPage(db, root, p);
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

    public void remove(RectangleRn r, T obj) {
        if (root == null) { 
            throw new StorageError(StorageError.KEY_NOT_FOUND);
        }
        ArrayList reinsertList = new ArrayList();
        int reinsertLevel = root.remove(r, obj, height, reinsertList);
        if (reinsertLevel < 0) { 
             throw new StorageError(StorageError.KEY_NOT_FOUND);
        }        
        for (int i = reinsertList.size(); --i >= 0;) {
            RtreeRnPage p = (RtreeRnPage)reinsertList.get(i);
            for (int j = 0, n = p.n; j < n; j++) { 
                RtreeRnPage q = root.insert(getStorage(), p.b[j], p.branch.get(j), height - reinsertLevel); 
                if (q != null) { 
                    // root splitted
                    root = new RtreeRnPage(getStorage(), root, q);
                    height += 1;
                }
            }
            reinsertLevel -= 1;
            p.deallocate();
        }
        if (root.n == 1 && height > 1) { 
            RtreeRnPage newRoot = (RtreeRnPage)root.branch.get(0);
            root.deallocate();
            root = newRoot;
            height -= 1;
        }
        n -= 1;
        updateCounter += 1;
        modify();
    }
    
    public Object[] get(RectangleRn r) {
        ArrayList result = new ArrayList();
        if (root != null) { 
            root.find(r, result, height);
        }
        return result.toArray();
    }

    public ArrayList<T> getList(RectangleRn r) { 
        ArrayList<T> result = new ArrayList<T>();
        if (root != null) { 
            root.find(r, result, height);
        }
        return result;
    }

    public RectangleRn getWrappingRectangle() {
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
        RtreeIterator(RectangleRn r) { 
            counter = updateCounter;
            if (height == 0) { 
                return;
            }
            this.r = r;            
            pageStack = new RtreeRnPage[height];
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
        
        private boolean gotoFirstItem(int sp, RtreeRnPage pg) { 
            for (int i = 0, n = pg.n; i < n; i++) { 
                if (r.intersects(pg.b[i])) { 
                    if (sp+1 == height || gotoFirstItem(sp+1, (RtreeRnPage)pg.branch.get(i))) { 
                        pageStack[sp] = pg;
                        posStack[sp] = i;
                        return true;
                    }
                }
            }
            return false;
        }
              
 
        private boolean gotoNextItem(int sp) {
            RtreeRnPage pg = pageStack[sp];
            for (int i = posStack[sp], n = pg.n; ++i < n;) { 
                if (r.intersects(pg.b[i])) { 
                    if (sp+1 == height || gotoFirstItem(sp+1, (RtreeRnPage)pg.branch.get(i))) { 
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

        RtreeRnPage[] pageStack;
        int[]         posStack;
        int           counter;
        RectangleRn   r;
    }
    
    static class RtreeEntry<T> implements Map.Entry<RectangleRn,T> {
        RtreeRnPage pg;
        int         pos;

	public RectangleRn getKey() {
	    return pg.b[pos];
	}

	public T getValue() {
	    return (T)pg.branch.get(pos);
	}

  	public T setValue(T value) {
            throw new UnsupportedOperationException();
        }

        RtreeEntry(RtreeRnPage pg, int pos) { 
            this.pg = pg;
            this.pos = pos;
        }
    }
        
    class RtreeEntryIterator extends RtreeIterator<Map.Entry<RectangleRn,T>> {
        RtreeEntryIterator(RectangleRn r) { 
            super(r);
        }
        
        protected Object current(int sp) { 
            return new RtreeEntry(pageStack[sp], posStack[sp]);
        }
    }

    public Iterator<T> iterator() {
        return iterator(getWrappingRectangle());
    }

    public IterableIterator<Map.Entry<RectangleRn,T>> entryIterator() {
        return entryIterator(getWrappingRectangle());
    }

    public IterableIterator<T> iterator(RectangleRn r) { 
        return new RtreeIterator<T>(r);
    }

    public IterableIterator<Map.Entry<RectangleRn,T>> entryIterator(RectangleRn r) { 
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
        PointRn center;
        Storage storage;

        NeighborIterator(PointRn center) { 
            this.center = center;
            counter = updateCounter;
            if (height == 0) { 
                return;
            }
            list = new Neighbor(root, root.cover().distance(center), height);
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
                RtreeRnPage pg = (RtreeRnPage)(neighbor.child instanceof RtreeRnPage ? neighbor.child : storage.getObjectByOID(storage.getOid(neighbor.child)));
                for (int i = 0, n = pg.n; i < n; i++) { 
                    insert(new Neighbor(pg.branch.get(i), pg.b[i].distance(center), neighbor.level-1));
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
        
    public IterableIterator<T> neighborIterator(PointRn center) { 
        return new NeighborIterator(center);
    }
}
    

