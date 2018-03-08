package org.garret.rdf;

import java.util.*;
import org.garret.perst.*;

/**
 * Main class
 **/
public class VersionedStorage 
{ 
    Storage      db;
    DatabaseRoot root;

    private static final Iterator EmptyIterator = (new HashSet()).iterator();

    /** 
     * List of separator characters used to split string into keywords
     **/ 
    public static char[] keywordSeparators = {' ', ','};

    /** 
     * List of most commonly used words which should be ignored andnot included in inverse index
     */ 
    public static HashSet keywordStopList = new HashSet();

    static {
        keywordStopList.add("the");
        keywordStopList.add("at");
        keywordStopList.add("of");
        keywordStopList.add("a");
        keywordStopList.add("to");
        keywordStopList.add("at");
        keywordStopList.add("and");
        keywordStopList.add("or");
        keywordStopList.add("i");
    }
    
    /**
     * Open database
     * @param filePath path to the database file
     */
     public void open(String filePath) { 
         db = StorageFactory.getInstance().createStorage(); 
         db.open(filePath);
         root = (DatabaseRoot)db.getRoot();
         if (root == null) {
             root = new DatabaseRoot();
             root.prefixUriIndex = db.createIndex(String.class, true);
             root.suffixUriIndex = db.createIndex(String.class, true);
             root.strPropIndex = db.createIndex(new Class[]{PropDef.class, String.class}, false);
             root.numPropIndex = db.createIndex(new Class[]{PropDef.class, Double.class}, false);
             root.refPropIndex = db.createIndex(new Class[]{PropDef.class, VersionHistory.class}, false);
             root.timePropIndex = db.createIndex(new Class[]{PropDef.class, Date.class}, false);
             root.propDefIndex = db.createFieldIndex(PropDef.class, "name", true);            
             root.timeIndex = db.createFieldIndex(Thing.class, "timestamp", false);
             root.inverseIndex = db.createIndex(String.class, false);
             root.spatialIndex = db.createSpatialIndexR2();
             root.latest = db.createSet();
             createMetaType();
             db.setRoot(root);
         }
     }
    
    /**
     * Get verion history by URI
     * @param uri object URI
     * @return version history or null if no such object is found
     */
    public VersionHistory getObject(String uri) {
        return (VersionHistory)root.prefixUriIndex.get(uri);
    }

    /**
     * Get latest verion of object with specified URI
     * @param uri object URI
     * @return latest version of object or null if no such object is found     
     */
    public Thing getLatestVersion(String uri) {
        VersionHistory vh = (VersionHistory)root.prefixUriIndex.get(uri);
        return (vh != null) ? vh.getLatest() : null;
    }        

    /**
     * Get verion history by URI and timestamp
     * @param uri object URI
     * @param kind search kind, should be object SearchKind.LatestVersion, SearchKind.LatestBefore or 
     * SearchKind.OldestAfter
     * @param timestamp timestamp used to locate version
     * @return version of the object or null if no such version is found
     */
     public Thing getVersion(String uri, SearchKind kind, Date timestamp) {
         VersionHistory vh = (VersionHistory)root.prefixUriIndex.get(uri);
         if (vh != null) { 
             return vh.getVersion(kind, timestamp);
         }
         return null;
     }
    
    /**
     * Create bew object. If version history with this URI is not exists, it is created first.
     * Then new object version is created and appended to this version history.
     * 
     * @param uri object URI
     * @param type URI of object type
     * @param props object properties
     * @return created object version
     */
     public Thing createObject(String uri, String type, NameVal[] props) {
         VersionHistory vh = (VersionHistory)root.prefixUriIndex.get(uri);
         if (vh == null) {
             VersionHistory typeVh = null;
             typeVh = getObject(type);
             if (typeVh == null) { 
                 typeVh = createVersionHistory(type, root.metatype);
                 createObject(root.metatype.getLatest(), typeVh, new NameVal[0]);
             }
             vh = createVersionHistory(uri, typeVh);
         } else { 
             root.latest.remove(vh.getLatest());
         }
         return createObject(vh.type.getLatest(), vh, props); 
     }

    /**
     * Get iterator through object matching specified search parameters
     * @param type String representing type of the object (direct or indirect - IsInstanceOf
     * method will be used to check if object belongs to the specified type). It may be null, 
     * in this case type criteria is skipped.
     * @param uri Object URI pattern. It may be null, in this case URI is not inspected.
     * @param patterns array of name:value pairs specifying search condition for object properties
     * @param kind search kind used to select inspected versions
     * @param timestamp timestamp used to select versions, if kind is SearchKind.LatestVersion
     * or SearchKind.AllVersions this parameter is ignored
     * @return Enumerator through object meet search criteria.
     */
     public Iterator search(String type, String uri, NameVal[] patterns, SearchKind kind, Date timestamp) {
         VersionHistory typeVh = null;
         root.sharedLock();
         try {
             if (type != null) { 
                 typeVh = getObject(type);
                 if (typeVh == null) { 
                     return EmptyIterator;
                 }
             }
             if (uri != null) {
                 int wc = uri.indexOf('*');
                 if (wc < 0) { 
                     Key key = new Key(uri);
                     return new SearchResult(root, typeVh, null, patterns, kind, timestamp, root.prefixUriIndex.iterator(key, key, Index.ASCENT_ORDER));
                 } else if (wc > 0) { 
                     String prefix = uri.substring(0, wc);
                     return new SearchResult(root, typeVh, uri, patterns, kind, timestamp, root.prefixUriIndex.prefixIterator(prefix));
                 } else if ((wc = uri.lastIndexOf('*')) < uri.length()-1) {
                     String suffix = reverseString(uri.substring(wc+1, uri.length()));
                     return new SearchResult(root, typeVh, uri, patterns, kind, timestamp, root.suffixUriIndex.prefixIterator(suffix));
                 }
             }
             if (patterns.length > 0) { 
                 NameVal prop = patterns[0];
                 Object val = prop.val;
                 NameVal[] restOfPatterns = subArray(patterns);

                 if (Symbols.Timestamp.equals(prop.name)) { 
                     if (val instanceof Range) { 
                         Range range = (Range)val;
                         if (range.from instanceof Date) {
                             Key fromKey = new Key((Date)range.from, range.fromInclusive);
                             Key tillKey = new Key((Date)range.till, range.tillInclusive);
                             return new SearchResult(root, typeVh, uri, restOfPatterns, kind, timestamp, 
                                                     root.timeIndex.iterator(fromKey, tillKey, Index.ASCENT_ORDER));
                         }
                     } else if (val instanceof Date) {
                         Key key = new Key((Date)val);
                         return new SearchResult(root, typeVh, uri, restOfPatterns, kind, timestamp, 
                                                 root.timeIndex.iterator(key, key, Index.ASCENT_ORDER));                            
                     } 
                     return EmptyIterator;
                 } else if (Symbols.Rectangle.equals(prop.name)) {
                     if (val instanceof NameVal[]) {
                         NameVal[] coord = (NameVal[])val;
                         if (coord.length == 4) {
                             RectangleR2 r = new RectangleR2(((Number)coord[0].val).doubleValue(), 
                                                             ((Number)coord[1].val).doubleValue(), 
                                                             ((Number)coord[2].val).doubleValue(), 
                                                             ((Number)coord[3].val).doubleValue());
                             return new SearchResult(root, typeVh, uri, restOfPatterns, kind, timestamp, 
                                                     root.spatialIndex.iterator(r));
                         }
                     }
                 } else if (Symbols.Point.equals(prop.name)) {
                     if (val instanceof NameVal[]) {
                         NameVal[] coord = (NameVal[])val;
                         if (coord.length == 2) {
                             double x = ((Number)coord[0].val).doubleValue();
                             double y = ((Number)coord[1].val).doubleValue();
                             RectangleR2 r = new RectangleR2(x, y, x, y);
                             return new SearchResult(root, typeVh, uri, restOfPatterns, kind, timestamp, 
                                                     root.spatialIndex.iterator(r));
                         }
                     }
                 } else if (Symbols.Keyword.equals(prop.name)) {
                     if (val instanceof String) {
                         ArrayList keywords = getKeywords((String)val);
                         Iterator[] occurences = new Iterator[keywords.size()];
                         for (int i = 0; i < occurences.length; i++) { 
                             Key key = new Key((String)keywords.get(i));
                             occurences[i] = root.inverseIndex.iterator(key, key, Index.ASCENT_ORDER);
                         }
                         return new SearchResult(root, typeVh, uri, restOfPatterns, kind, timestamp, db.merge(occurences));
                     }
                 }

                 PropDef def = (PropDef)root.propDefIndex.get(prop.name);
                 if (def == null) { 
                     return EmptyIterator;
                 }
                 if (val instanceof Range) { 
                     Range range = (Range)val;
                     if (range.from instanceof Number) {
                         Key fromKey = new Key(new Object[]{def, range.from}, range.fromInclusive);
                         Key tillKey = new Key(new Object[]{def, range.till}, range.tillInclusive);
                         return new SearchResult(root, typeVh, uri, restOfPatterns, kind, timestamp, 
                                                 root.numPropIndex.iterator(fromKey, tillKey, Index.ASCENT_ORDER));
                     } else if (range.from instanceof Date) {
                         Key fromKey = new Key(new Object[]{def, range.from}, range.fromInclusive);
                         Key tillKey = new Key(new Object[]{def, range.till}, range.tillInclusive);
                         return new SearchResult(root, typeVh, uri, restOfPatterns, kind, timestamp, 
                                                 root.timePropIndex.iterator(fromKey, tillKey, Index.ASCENT_ORDER));
                     } else { 
                         Key fromKey = new Key(new Object[]{def, range.from}, range.fromInclusive);
                         Key tillKey = new Key(new Object[]{def, range.till}, range.tillInclusive);
                         return new SearchResult(root, typeVh, uri, restOfPatterns, kind, timestamp, 
                                                 root.strPropIndex.iterator(fromKey, tillKey, Index.ASCENT_ORDER));
                     }
                 } else if (val instanceof String) {
                     String str = (String)val;
                     int wc = str.indexOf('*');
                     if (wc < 0) { 
                         Key key = new Key(new Object[]{def, str});
                         return new SearchResult(root, typeVh, uri, restOfPatterns, kind, timestamp, 
                                                 root.strPropIndex.iterator(key, key, Index.ASCENT_ORDER));
                         
                     } else if (wc > 0) { 
                         String prefix = str.substring(0, wc);
                         Key fromKey = new Key(new Object[]{def, prefix});
                         Key tillKey = new Key(new Object[]{def, prefix + Character.MAX_VALUE}, false);                        
                         return new SearchResult(root, typeVh, uri, wc == str.length()-1 ? restOfPatterns : patterns, kind, timestamp, 
                                                 root.strPropIndex.iterator(fromKey, tillKey, Index.ASCENT_ORDER));
                     }                             
                 } else if (val instanceof Number) {
                     Key key = new Key(new Object[]{def, val});
                     return new SearchResult(root, typeVh, uri, restOfPatterns, kind, timestamp, 
                                             root.numPropIndex.iterator(key, key, Index.ASCENT_ORDER));
                 } else if (val instanceof Date) {
                     Key key = new Key(new Object[]{def, val});
                     return new SearchResult(root, typeVh, uri, restOfPatterns, kind, timestamp, 
                                             root.timePropIndex.iterator(key, key, Index.ASCENT_ORDER));
                 } else if (val instanceof NameVal) { 
                     Iterator iterator = searchReferenceProperty(typeVh, uri, patterns, kind, timestamp, (NameVal)val, false, def, new ArrayList());
                     if (iterator != null) {
                         return iterator;
                     }
                 } else if (val instanceof NameVal[]) { 
                     NameVal[] props = (NameVal[])val;
                     if (props.length > 0) {
                         Iterator iterator = searchReferenceProperty(typeVh, uri, patterns, kind, timestamp, props[0], props.length > 1, def, new ArrayList());
                         if (iterator != null) {
                             return iterator;
                         }
                     }
                 }                 
             }
             if (kind == SearchKind.LatestVersion) { 
                 return new SearchResult(root, typeVh, uri, patterns, kind, timestamp, root.latest.iterator());   
             }
             return new SearchResult(root, typeVh, uri, patterns, kind, timestamp, root.timeIndex.iterator());           
         } finally { 
             root.unlock();
         }
     }
    
    class ReferenceIterator implements Iterator {
        PropDef[]     defs;
        Iterator[]    iterators;
        int           pos;
        Thing         currThing;
        SearchKind    kind;
        Date          timestamp; 
        DatabaseRoot  root;
        HashSet       visited;

        public ReferenceIterator(DatabaseRoot root, PropDef[] defs, Iterator iterator, SearchKind kind, Date timestamp) {
            this.root = root;
            this.defs = defs;
            this.kind = kind;
            this.timestamp = timestamp;
            iterators = new Iterator[defs.length+1];
            iterators[iterators.length-1] = iterator;
            visited = new HashSet();
            pos = iterators.length-1;
            gotoNext();
        }
        
        public boolean hasNext() {
            return currThing != null;
        }
        
        public Object next() {
            if (currThing == null) { 
                throw new NoSuchElementException();
            }
            Thing thing = currThing;
            gotoNext();
            return thing;
        }

        public void remove() { 
            throw new UnsupportedOperationException();
        }

        private void gotoNext() {
            while (true) {
                while (pos < iterators.length && !iterators[pos].hasNext()) {
                    pos += 1;
                }
                if (pos == iterators.length) { 
                    currThing = null;
                    return;
                } 
                Thing thing = (Thing)iterators[pos].next();
                if (kind == SearchKind.LatestVersion) { 
                    if (!thing.isLatest()) {
                        continue;
                    }
                } else if (kind == SearchKind.LatestBefore) { 
                    if (thing.timestamp.compareTo(timestamp) > 0) { 
                        continue;
                    }
                } else if (kind == SearchKind.OldestAfter) { 
                    if (thing.timestamp.compareTo(timestamp) < 0) { 
                        continue;
                    }
                }
                if (pos == 0) { 
                    Integer oid = new Integer(thing.getOid());
                    if (visited.contains(oid)) { 
                        continue;
                    } else {
                        visited.add(oid);
                    }
                    currThing = thing;
                    return;
                }
                pos -= 1;
                Key key = new Key(new Object[]{defs[pos], thing.vh});
                iterators[pos] = root.refPropIndex.iterator(key, key, Index.ASCENT_ORDER);
            }
        }
    }
       
    private Iterator searchReferenceProperty(VersionHistory type, String uri, NameVal[] patterns, SearchKind kind, Date timestamp, NameVal prop, boolean compound, PropDef def, ArrayList refs)
    {
        refs.add(def);

        NameVal[] restOfPatterns = compound ? patterns : subArray(patterns);
        PropDef[] refProps = (PropDef[])refs.toArray(new PropDef[refs.size()]); 
    
        Object val = prop.val;
        if (Symbols.Timestamp.equals(prop.name)) { 
            if (val instanceof Range) { 
                Range range = (Range)val;
                if (range.from instanceof Date) {
                    Key fromKey = new Key((Date)range.from, range.fromInclusive);
                    Key tillKey = new Key((Date)range.till, range.tillInclusive);
                    return new SearchResult(root, type, uri, restOfPatterns, kind, timestamp, 
                                            new ReferenceIterator(root, refProps, 
                                                                  root.timeIndex.iterator(fromKey, tillKey, Index.ASCENT_ORDER), 
                                                                  kind, timestamp));
                }
            }  else if (val instanceof Date) {
                Key key = new Key((Date)val);
                return new SearchResult(root, type, uri, restOfPatterns, kind, timestamp, 
                                        new ReferenceIterator(root, refProps, 
                                                              root.timeIndex.iterator(key, key, Index.ASCENT_ORDER), 
                                                              kind, timestamp));                            
            } 
            return EmptyIterator;
        } else if (Symbols.Rectangle.equals(prop.name)) { 
            if (val instanceof NameVal[]) {
                NameVal[] coord = (NameVal[])val;
                if (coord.length == 4) {
                    RectangleR2 r = new RectangleR2(((Number)coord[0].val).doubleValue(), 
                                                    ((Number)coord[1].val).doubleValue(), 
                                                    ((Number)coord[2].val).doubleValue(), 
                                                    ((Number)coord[3].val).doubleValue());
                    return new SearchResult(root, type, uri, restOfPatterns, kind, timestamp, 
                                            new ReferenceIterator(root, refProps, 
                                                                  root.spatialIndex.iterator(r), kind, timestamp));
                }
            }
        } else if (Symbols.Point.equals(prop.name)) { 
            if (val instanceof NameVal[]) {
                NameVal[] coord = (NameVal[])val;
                if (coord.length == 2) {
                    double x = ((Number)coord[0].val).doubleValue();
                    double y = ((Number)coord[1].val).doubleValue();
                    RectangleR2 r = new RectangleR2(x, y, x, y);
                    return new SearchResult(root, type, uri, restOfPatterns, kind, timestamp, 
                                            new ReferenceIterator(root, refProps, 
                                                                  root.spatialIndex.iterator(r), kind, timestamp));
                }
            }
        } else if (Symbols.Keyword.equals(prop.name)) { 
            if (val instanceof String) {
                ArrayList keywords = getKeywords((String)val);
                Iterator[] occurences = new Iterator[keywords.size()];
                for (int i = 0; i < occurences.length; i++) { 
                    Key key = new Key((String)keywords.get(i));
                    occurences[i] = root.inverseIndex.iterator(key, key, Index.ASCENT_ORDER);
                }
                return new SearchResult(root, type, uri, restOfPatterns, kind, timestamp, 
                                        new ReferenceIterator(root, refProps, 
                                                              db.merge(occurences), kind, timestamp));
            }
        }


        def = (PropDef)root.propDefIndex.get(prop.name);
        if (def == null) { 
            return EmptyIterator;
        }
        if (val instanceof Range) { 
            Range range = (Range)val;
            if (range.from instanceof Number) {
                Key fromKey = new Key(new Object[]{def, range.from}, range.fromInclusive);
                Key tillKey = new Key(new Object[]{def, range.till}, range.tillInclusive);
                return new SearchResult(root, type, uri, restOfPatterns, kind, timestamp, 
                                        new ReferenceIterator(root, refProps, 
                                                              root.numPropIndex.iterator(fromKey, tillKey, Index.ASCENT_ORDER),
                                                              kind, timestamp));
            } else if (range.from instanceof Date) {
                Key fromKey = new Key(new Object[]{def, range.from}, range.fromInclusive);
                Key tillKey = new Key(new Object[]{def, range.till}, range.tillInclusive);
                return new SearchResult(root, type, uri, restOfPatterns, kind, timestamp, 
                                        new ReferenceIterator(root, refProps, 
                                                              root.timePropIndex.iterator(fromKey, tillKey, Index.ASCENT_ORDER), 
                                                              kind, timestamp));
            } else { 
                Key fromKey = new Key(new Object[]{def, range.from}, range.fromInclusive);
                Key tillKey = new Key(new Object[]{def, range.till}, range.tillInclusive);
                return new SearchResult(root, type, uri, restOfPatterns, kind, timestamp, 
                                        new ReferenceIterator(root, refProps, 
                                                              root.strPropIndex.iterator(fromKey, tillKey, Index.ASCENT_ORDER), 
                                                              kind, timestamp));
            }
        } 
        if (val instanceof String) {
            String str = (String)prop.val;
            int wc = str.indexOf('*');
            if (wc < 0) { 
                Key key = new Key(new Object[]{def, str});
                return new SearchResult(root, type, uri, restOfPatterns, kind, timestamp, 
                                        new ReferenceIterator(root, refProps, 
                                                              root.strPropIndex.iterator(key, key, Index.ASCENT_ORDER), kind, timestamp));
            } else if (wc > 0) { 
                String prefix = str.substring(0, wc);
                Key fromKey = new Key(new Object[]{def, prefix});
                Key tillKey = new Key(new Object[]{def, prefix + Character.MAX_VALUE}, false);                        
                return new SearchResult(root, type, uri, wc == str.length()-1 ? restOfPatterns : patterns, kind, timestamp, 
                                        new ReferenceIterator(root, refProps, 
                                                              root.strPropIndex.iterator(fromKey, tillKey, Index.ASCENT_ORDER), 
                                                              kind, timestamp));
            } 
        } 
        else if (val instanceof Number) {
            Key key = new Key(new Object[]{def, val});
            return new SearchResult(root, type, uri, restOfPatterns, kind, timestamp, 
                                    new ReferenceIterator(root, refProps, 
                                                          root.numPropIndex.iterator(key, key, Index.ASCENT_ORDER), 
                                                          kind, timestamp));
        } else if (val instanceof Date) {
            Key key = new Key(new Object[]{def, val});
            return new SearchResult(root, type, uri, restOfPatterns, kind, timestamp, 
                                    new ReferenceIterator(root, refProps, 
                                                          root.timePropIndex.iterator(key, key, Index.ASCENT_ORDER), 
                                                          kind, timestamp));
        } else if (val instanceof NameVal) {
            return searchReferenceProperty(type, uri, patterns, kind, timestamp, (NameVal)val, compound, def, refs);
        } else if (val instanceof NameVal[]) {
            NameVal[] props = (NameVal[])val;
            if (props.length > 0) {
                return searchReferenceProperty(type, uri, patterns, kind, timestamp, props[0], true, def, refs);
            }
        }
        return null;
    }
        
    /**
     * Close database
     */
    public void close() {
        db.close();
    }

    /**
     * Commit current transaction
     */
    public void commit() {
        db.commit();
        root.unlock();
    }
    
    /**
     * Rollback current transaction
     */
    public void rollback() {
        db.rollback();
        root.unlock();
    }

    /** 
     * Begin new write transction: set exclusive lock
     */ 
     public void beginTransaction() {
         root.exclusiveLock();
     }

     static class SearchResult implements Iterator {
         VersionHistory type;
         String         uri;
         NameVal[]      patterns;
         SearchKind     kind;
         Date           timestamp;
         Iterator       iterator;
         Thing          currThing;
         int            currVersion;
         Link           currHistory;
         DatabaseRoot   root;
         
         public SearchResult(DatabaseRoot root, VersionHistory type, String uri, NameVal[] patterns, SearchKind kind, Date timestamp, Iterator iterator) 
         {
             this.root = root;
             this.type = type;    
             this.uri = uri;    
             this.patterns = patterns;    
             this.kind = kind;    
             this.timestamp = timestamp;    
             this.iterator = iterator;    
             gotoNext();
         }
         
         
         public boolean hasNext() {
             return currThing != null;
         }
         
         public Object next() {
             if (currThing == null) { 
                 throw new NoSuchElementException();
             }
             Thing thing = currThing;
             gotoNext();
             return thing;
         }
         
         
         public void remove() { 
             throw new UnsupportedOperationException();
         }

         private void gotoNext() {
           Repeat:
             while (true) { 
                 if (currHistory != null) { 
                     while (currVersion < currHistory.size()) { 
                         Thing thing = (Thing)currHistory.get(currVersion++);
                         if (match(thing)) { 
                             return;
                         }
                     }
                     currHistory = null;
                 }              
                 while (iterator.hasNext()) { 
                     Object curr = iterator.next();
                     if (curr instanceof Thing) { 
                         if (match((Thing)curr)) { 
                             return;
                         }
                     } else if (curr instanceof VersionHistory) { 
                         currHistory = ((VersionHistory)curr).versions;
                         currVersion = 0;
                         continue Repeat;
                     }                    
                 }
                 currThing = null;
                 return;
             }
         }

         private static boolean matchString(String str, String pat) { 
             if (pat.indexOf('*') < 0) { 
                 return  pat.equals(str); 
             }
             int pi = 0, si = 0, pn = pat.length(), sn = str.length(); 
             int wildcard = -1, strpos = -1;
             while (true) { 
                 if (pi < pn && pat.charAt(pi) == '*') { 
                     wildcard = ++pi;
                     strpos = si;
                 } else if (si == sn) { 
                     return pi == pn;
                 } else if (pi < pn && str.charAt(si) == pat.charAt(pi)) {
                     si += 1;
                     pi += 1;
                 } else if (wildcard >= 0) { 
                     si = ++strpos;
                     pi = wildcard;
                 } else { 
                     return false;
                 }
             }
         }
         
         private boolean match(Thing thing) {              
             if (type != null && !thing.isInstanceOf(type, kind, timestamp)) {
                 return false;
             }
             if (kind == SearchKind.LatestVersion) { 
                 if (!thing.isLatest()) { 
                     return false;
                 }
             } else if (kind == SearchKind.LatestBefore) { 
                 if (thing.timestamp.compareTo(timestamp) > 0 || thing.vh.getLatestBefore(timestamp) != thing) {
                     return false;
                 }
             } else if (kind == SearchKind.OldestAfter) { 
                 if (thing.timestamp.compareTo(timestamp) < 0 || thing.vh.getOldestAfter(timestamp) != thing) {
                     return false;
                 }
             }

             if (uri != null) { 
                 if (!matchString(thing.vh.uri, uri)) { 
                     return false;
                 }
             }
             for (int i = 0; i < patterns.length; i++) { 
                 if (!matchProperty(patterns[i], thing)) { 
                     return false;
                 }
             }
             currThing = thing;
             return true;
         }

         private boolean matchProperty(NameVal prop, Thing thing) {
             if (Symbols.Point.equals(prop.name)) {
                 if (prop.val instanceof NameVal[]) {
                     NameVal[] coord = (NameVal[])prop.val;
                     if (coord.length == 2) { 
                         double x = ((Number)coord[0].val).doubleValue();
                         double y = ((Number)coord[1].val).doubleValue();
                         RectangleR2 r = new RectangleR2(x, y, x, y);
                         Iterator i = root.spatialIndex.iterator(r);
                         while (i.hasNext()) { 
                             if (i.next() == thing) {
                                 return true;   
                             }
                         }
                         return false;
                     }
                 }                 
             } else if (Symbols.Rectangle.equals(prop.name)) {
                 if (prop.val instanceof NameVal[]) {
                     NameVal[] coord = (NameVal[])prop.val;
                     if (coord.length == 4) { 
                         RectangleR2 r = new RectangleR2(((Number)coord[0].val).doubleValue(),
                                                         ((Number)coord[1].val).doubleValue(),
                                                         ((Number)coord[2].val).doubleValue(),
                                                         ((Number)coord[3].val).doubleValue());
                         Iterator i = root.spatialIndex.iterator(r);
                         while (i.hasNext()) { 
                             if (i.next() == thing) {
                                 return true;   
                             }
                         }
                         return false;
                     }
                 }      
             } else if (Symbols.Keyword.equals(prop.name)) {
                 if (prop.val instanceof String) {
                     HashSet allKeywords = new HashSet();
                     for (int i = 0; i < thing.props.length; i++) { 
                         PropVal pv = thing.props[i];
                         Object val = pv.val;
                         if (val instanceof String) {
                             ArrayList keywords = getKeywords((String)val);
                             allKeywords.addAll(keywords);
                         }
                     }
                     ArrayList keywords = getKeywords((String)prop.val);
                     return allKeywords.containsAll(keywords);
                 }
             }
             Object[] propVal = thing.get(prop.name); 
            NextItem:
             for (int i = 0; i < propVal.length; i++) {
                 Object val = propVal[i];
                 Object pattern = prop.val;
                 if (val instanceof String && pattern instanceof String) { 
                     if (matchString((String)val, (String)pattern)) {
                         return true;
                     }
                 } else if (pattern instanceof NameVal) { 
                     if (val instanceof VersionHistory
                         && followReference((NameVal)pattern, (VersionHistory)val)) 
                     { 
                         return true;
                     }
                 } else if (pattern instanceof NameVal[]) {
                     if (val instanceof VersionHistory) { 
                         NameVal[] arr = (NameVal[])prop.val;
                         for (int j = 0; j < arr.length; j++) {
                             if (!followReference(arr[j], (VersionHistory)val)){
                                 continue NextItem;
                             }
                         }
                         return true;
                     } 
                 } else if (pattern instanceof Range && val instanceof Comparable) {
                     try {
                         Range range = (Range)pattern;
                         Comparable cmp = (Comparable)val;
                         return cmp.compareTo(range.from) >= (range.fromInclusive ? 0 : 1) &&
                             cmp.compareTo(range.till) <= (range.tillInclusive ? 0 : -1);
                     } catch (ClassCastException x) {}
                 } else if (pattern != null && pattern.equals(val)) {
                     return true;
                 }
             }
             return false;
         }

         private boolean followReference(NameVal prop, VersionHistory vh) {
             if (vh != null) { 
                 if (kind == SearchKind.AllVersions)  { 
                     for (int i = 0, n = vh.versions.size(); i < n; i++) { 
                         Thing v = (Thing)vh.versions.get(i);
                         if (matchProperty(prop, v)) {
                             return true;
                         }
                     }
                 } else { 
                     Thing thing = vh.getVersion(kind, timestamp);
                     return thing != null && matchProperty(prop, thing); 
                 }
             }
             return false;
         }
     }
    
    
    private void createMetaType() {
        VersionHistory vh = createVersionHistory(Symbols.Metatype, null);
        vh.type = vh;
        Thing metatype = createObject(null, vh, new NameVal[0]);
        metatype.type = metatype;
        root.metatype = vh;
    }

    private static String reverseString(String s) { 
        int len = s.length();
        char[] chars = new char[len];
        for (int i = 0; i < len; i++) { 
            chars[i] = s.charAt(len-i-1);
        }
        return new String(chars);
    }

        
    private VersionHistory createVersionHistory(String uri, VersionHistory type) {
        VersionHistory vh = new VersionHistory();
        vh.uri = uri;
        vh.type = type;
        vh.versions = db.createLink();
        root.prefixUriIndex.put(uri, vh);
        root.suffixUriIndex.put(reverseString(uri), vh);
        return vh;
    }

    private static NameVal[] subArray(NameVal[] arr) {
        NameVal[] newArr = new NameVal[arr.length-1];
        System.arraycopy(arr, 1, newArr, 0, newArr.length);
        return newArr;
    }

    private Thing createObject(Thing type, VersionHistory vh, NameVal[] props) {
        Thing thing = new Thing();
        thing.vh = vh;
        thing.type = type;
        thing.timestamp = new Date();
        thing.props = new PropVal[props.length];
        for (int i = 0; i < props.length; i++) { 
            thing.props[i] = new PropVal();
        }   
        for (int i = 0; i < props.length; i++) { 
            NameVal prop = props[i];
            PropDef def = (PropDef)root.propDefIndex.get(prop.name);
            if (def == null) {
                def = new PropDef();
                def.name = prop.name;
                root.propDefIndex.put(def);
            }
            Object val = prop.val;
            PropVal pv = thing.props[i];
            pv.def = def;
            pv.val = val;
            Key key = new Key(new Object[]{def, val});
            if (val instanceof String) { 
                root.strPropIndex.put(key, thing);
                ArrayList keywords = getKeywords((String)val);
                for (int j = keywords.size(); --j >= 0;) { 
                    root.inverseIndex.put((String)keywords.get(j), thing);
                }
            } else if (val instanceof Number) { 
                root.numPropIndex.put(key, thing);
            } else if (val instanceof Date) { 
                root.timePropIndex.put(key, thing);
            } else if (val instanceof VersionHistory || val == null) { 
                root.refPropIndex.put(key, thing);
                if (Symbols.Rectangle.equals(prop.name)) {
                    PropVal[] coord = ((VersionHistory)val).getLatest().props;
                    RectangleR2 r = new RectangleR2(((Number)coord[0].val).doubleValue(), 
                                                    ((Number)coord[1].val).doubleValue(), 
                                                    ((Number)coord[2].val).doubleValue(), 
                                                    ((Number)coord[3].val).doubleValue());
                    root.spatialIndex.put(r, thing);   
                } else if (Symbols.Rectangle.equals(prop.name)) {
                    PropVal[] coord = ((VersionHistory)val).getLatest().props;
                    double x = ((Number)coord[0].val).doubleValue();
                    double y = ((Number)coord[1].val).doubleValue();
                    RectangleR2 r = new RectangleR2(x, y, x, y);
                    root.spatialIndex.put(r, thing);   
                }
            } else { 
                throw new IllegalArgumentException("Invalid propery value type " + prop.val.getClass());
            }
        }
        thing.modify();
        vh.versions.add(thing);
        root.timeIndex.put(thing);
        root.latest.add(thing);
        return thing;
    }

    private static ArrayList getKeywords(String str) {
        ArrayList list = new ArrayList();
        char[] separators = keywordSeparators;
        str = str.toLowerCase();
        int len = str.length();
        int p = 0;
        do { 
            int minNp = len;
            for (int i = 0; i < separators.length; i++) { 
                int np = str.indexOf(separators[i], p);
                if (np >= 0 && np < minNp) { 
                    minNp = np;
                }
            }
            if (minNp-p > 1) { 
                String keyword = str.substring(p, minNp);
                if (!keywordStopList.contains(keyword)) { 
                    list.add(keyword);
                }
            }
            p = minNp + 1;
        } while (p < len);
        return list;
    }
}