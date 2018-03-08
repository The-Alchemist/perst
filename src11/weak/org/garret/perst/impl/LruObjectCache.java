package org.garret.perst.impl;
import  org.garret.perst.*;
import  java.lang.ref.*;

public class LruObjectCache implements OidHashTable { 
    Entry table[];
    static final float loadFactor = 0.75f;
    static final int defaultInitSize = 1319;
    int count;
    int threshold;
    int pinLimit;
    int nPinned;
    Entry pinList;
    Entry dirtyList;

    public void init(int initialCapacity, int nPinnedObjects) {
        if (initialCapacity == 0) { 
            initialCapacity = defaultInitSize;
        }
        threshold = (int)(initialCapacity * loadFactor);
        table = new Entry[initialCapacity];
        pinList = new Entry();
        dirtyList = new Entry();
        pinLimit = nPinnedObjects;
    }

    public synchronized boolean remove(int oid) {
        Entry tab[] = table;
        int index = (oid & 0x7FFFFFFF) % tab.length;
        for (Entry e = tab[index], prev = null; e != null; prev = e, e = e.next) {
            if (e.oid == oid) {
                if (prev != null) {
                    prev.next = e.next;
                } else {
                    tab[index] = e.next;
                }
                e.clear();
                unpinObject(e);
                count -= 1;
                return true;
            }
        }
        return false;
    }

    protected Reference createReference(Object obj) { 
        return new WeakReference(obj);
    }

    private final void unpinObject(Entry e) 
    {
        if (e.pin != null) { 
            e.unpin();
            nPinned -= 1;
        }
    }
        

    private final void pinObject(Entry e, IPersistent obj) 
    { 
        if (pinLimit != 0 && !obj.isModified()) { 
            if (e.pin != null) { 
                e.unlink();
            } else { 
                if (nPinned >= pinLimit && !pinList.isEmpty()) {
                    pinList.lru.unpin();
                } else { 
                    nPinned += 1;
                }
            }
            e.linkAfter(pinList, obj);
        }
    }

    public synchronized void put(int oid, IPersistent obj) { 
        Reference ref = createReference(obj);
        Entry tab[] = table;
        int index = (oid & 0x7FFFFFFF) % tab.length;
        for (Entry e = tab[index]; e != null; e = e.next) {
            if (e.oid == oid) {
                Assert.that(e.ref.get() == null);
                e.ref = ref;
                pinObject(e, obj);
                return;
            }
        }
        if (count >= threshold) {
            // Rehash the table if the threshold is exceeded
            rehash();
            tab = table;
            index = (oid & 0x7FFFFFFF) % tab.length;
        } 
        
        // Creates the new entry.
        tab[index] = new Entry(oid, ref, tab[index]);
        pinObject(tab[index], obj);
        count++;
    }

    public synchronized IPersistent get(int oid) {
        Entry tab[] = table;
        int index = (oid & 0x7FFFFFFF) % tab.length;
        for (Entry e = tab[index]; e != null; e = e.next) {
            if (e.oid == oid) {
                IPersistent obj = (IPersistent)e.ref.get();
                if (obj != null) { 
                    Assert.that(obj.getOid() == oid);
                    pinObject(e, obj);
                }
                return obj;
            }
        }
        return null;
    }
    
    public synchronized void reload() {
        for (int i = 0; i < table.length; i++) { 
            for (Entry e = table[i]; e != null; e = e.next) { 
                IPersistent obj = (IPersistent)e.ref.get();
                if (obj != null) { 
                    obj.invalidate();
                    try { 
                        //System.out.println("Reload object " + obj.getOid());
                        obj.load();
                    } catch (Exception x) { 
                        //x.printStackTrace();
                        // ignore errors caused by attempt to load object which was created in rollbacked transaction
                    }
                }
            }
        }
        dirtyList.prune();
    }

    public synchronized void flush() {
        while (!dirtyList.isEmpty()) { 
            IPersistent obj = (IPersistent)dirtyList.lru.ref.get();
            Assert.that(obj != null && obj.isModified());
            obj.store();
        }
        /*
        int nLiveObjects = 0;
        int nPinnedObjects = 0;
        for (int i = 0; i < table.length; i++) { 
            for (Entry e = table[i]; e != null; e = e.next) { 
                IPersistent obj = (IPersistent)e.ref.get();
                if (obj != null) { 
                    nLiveObjects += 1;
                    if (e.pin != null) { 
                        nPinnedObjects += 1;
                    }
                }
            }
        }
        System.out.println("nLiveObjects=" + nLiveObjects + ", nPinnedObjects:=" + nPinnedObjects + ", nPinned=" + nPinned + ", pinLimit=" + pinLimit);
        */
    }

    public synchronized void invalidate() {
        for (int i = 0; i < table.length; i++) { 
            for (Entry e = table[i]; e != null; e = e.next) { 
                IPersistent obj = (IPersistent)e.ref.get();
                if (obj != null) { 
                    unpinObject(e);
                    obj.invalidate();
                }
            }
            table[i] = null;
        }
        count = 0;
        nPinned = 0;
        pinList.prune();
        dirtyList.prune();
    }

    public synchronized void clear() {
        /* Explicit cleaning is not needed for this kind of cache 
        Entry tab[] = table;
        for (int i = 0; i < tab.length; i++) { 
            tab[i] = null;
        }
        count = 0;
        nPinned = 0;
        pinList.lru = pinList.mru = pinList;
        */
    }

    void rehash() {
        int oldCapacity = table.length;
        Entry oldMap[] = table;
        int i;
        for (i = oldCapacity; --i >= 0;) {
            Entry e, next, prev;
            for (prev = null, e = oldMap[i]; e != null; e = next) { 
                next = e.next;
                IPersistent obj = (IPersistent)e.ref.get();
                if (obj == null) {
                    Assert.that(e.pin == null);
                    count -= 1;
                    e.clear();
                    if (prev == null) { 
                        oldMap[i] = next;
                    } else { 
                        prev.next = next;
                    }
                } else { 
                    prev = e;
                }
            }
        }
        
        if (count <= (threshold >>> 1)) {
            return;
        }
        int newCapacity = oldCapacity * 2 + 1;
        Entry newMap[] = new Entry[newCapacity];

        threshold = (int)(newCapacity * loadFactor);
        table = newMap;

        for (i = oldCapacity; --i >= 0 ;) {
            for (Entry old = oldMap[i]; old != null; ) {
                Entry e = old;
                old = old.next;

                int index = (e.oid & 0x7FFFFFFF) % newCapacity;
                e.next = newMap[index];
                newMap[index] = e;
            }
        }
    }

    public synchronized void setDirty(IPersistent obj) {
        int oid = obj.getOid();
        Entry tab[] = table;
        int index = (oid & 0x7FFFFFFF) % tab.length;
        for (Entry e = tab[index]; e != null; e = e.next) {
            if (e.oid == oid) {
                Assert.that(e.ref.get() == obj);
                if (e.pin != null) { 
                    e.unlink();
                } else { 
                    nPinned += 1;
                }
                e.linkAfter(dirtyList, obj);
                return;
            }
        }
        Assert.that(false);
    }

    public synchronized void clearDirty(IPersistent obj) {
        int oid = obj.getOid();
        Entry tab[] = table;
        int index = (oid & 0x7FFFFFFF) % tab.length;
        for (Entry e = tab[index]; e != null; e = e.next) {
            if (e.oid == oid) {
                Assert.that(e.ref.get() == obj);
                unpinObject(e);
                return;
            }
        }
    }

    public int size() { 
        return count;
    }

    static class Entry { 
        Entry       next;
        Reference   ref;
        int         oid;
        Entry       lru;
        Entry       mru;
        IPersistent pin;

        void unlink() { 
            lru.mru = mru;
            mru.lru = lru;
        } 

        void unpin() { 
            unlink();
            lru = mru = null;
            pin = null;
        }

        void linkAfter(Entry head, IPersistent obj) { 
            mru = head.mru;
            mru.lru = this;
            head.mru = this;
            lru = head;
            pin = obj;
        }

        void clear() { 
            // !!!Bug at Nokia N95 and may be at some other Symbian-OS basedother phones: 
            // !!!weak references incorrectly work in case of clear() method invocations
            //ref.clear();
            ref = null;
            next = null;
        }

        boolean isEmpty() { 
            return mru == this;
        }

        void prune() { 
            mru = lru = this;
        }

        Entry(int oid, Reference ref, Entry chain) { 
            next = chain;
            this.oid = oid;
            this.ref = ref;
        }

        Entry() { 
            prune();
        }
    }
}

