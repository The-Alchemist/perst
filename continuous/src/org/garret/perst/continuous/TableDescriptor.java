
package org.garret.perst.continuous;

import java.lang.reflect.*;
import java.util.*;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DateTools;

import org.garret.perst.*;
import org.garret.perst.impl.ClassDescriptor;
import org.garret.perst.impl.LinkImpl;
import org.garret.perst.impl.StorageImpl;

class TableDescriptor extends Persistent implements Iterable<CVersionHistory>
{ 
    static class IndexDescriptor implements IValue
    {
        String  fieldName;
        int     fieldType;
        Index<VersionHistorySegment> index;    

        transient boolean unique;
        transient boolean caseInsensitive;
        transient Field field;

        IterableIterator<VersionHistorySegment> find(CVersion v) 
        { 
            Key key = extractKey(v);
            return key == null ? new EmptyIterator<VersionHistorySegment>() : index.iterator(key, key, GenericIndex.ASCENT_ORDER);
        }

        Key checkKey(Key key) 
        { 
            if (key != null) { 
                if (key.type == ClassDescriptor.tpObject && key.oval instanceof CVersion) { 
                    key = new Key(((CVersion)key.oval).getVersionHistory(), key.inclusion != 0);
                } else if (caseInsensitive && key.oval instanceof String) { 
                    key = new Key(((String)key.oval).toLowerCase(), key.inclusion != 0);
                }
            }
            return key;
        }

       void remove(VersionHistorySegment vhs, CVersion v) {
            index.remove(extractKey(v), vhs);
        }

        void add(CVersion v) 
        { 
            Key key = extractKey(v);
            if (key != null) { 
                index.put(key, new VersionHistorySegment(v));
            }
        }

        Key extractKey(CVersion obj) 
        { 
            try { 
                Field f = field;
                Key key = null;
                switch (fieldType) {
                case ClassDescriptor.tpBoolean:
                    key = new Key(f.getBoolean(obj));
                    break;
                case ClassDescriptor.tpByte:
                    key = new Key(f.getByte(obj));
                    break;
                case ClassDescriptor.tpShort:
                    key = new Key(f.getShort(obj));
                    break;
                case ClassDescriptor.tpChar:
                    key = new Key(f.getChar(obj));
                    break;
                case ClassDescriptor.tpInt:
                    key = new Key(f.getInt(obj));
                    break;            
                case ClassDescriptor.tpObject:
                {
                    IPersistent ptr = (IPersistent)f.get(obj);
                    if (ptr != null){ 
                        if (!ptr.isPersistent()) {
                            index.getStorage().makePersistent(ptr);
                        }                        
                        key = new Key(ptr);
                    }
                    break;
                }
                case ClassDescriptor.tpLong:
                    key = new Key(f.getLong(obj));
                    break;            
                case ClassDescriptor.tpDate:
                {
                    Date date = (Date)f.get(obj);
                    if (date != null) { 
                        key = new Key(date);
                    }
                    break;
                }
                case ClassDescriptor.tpFloat:
                    key = new Key(f.getFloat(obj));
                    break;
                case ClassDescriptor.tpDouble:
                    key = new Key(f.getDouble(obj));
                    break;
                case ClassDescriptor.tpEnum:
                {
                    Enum e = (Enum)f.get(obj);
                    if (e != null) { 
                        key = new Key(e);
                    }
                    break;
                }
                case ClassDescriptor.tpString:
                {
                    String s = (String)f.get(obj);
                    if (s != null) { 
                        if (caseInsensitive) { 
                            s = s.toLowerCase();
                        }
                        key = new Key(s.toCharArray());
                    }
                    break;
                }
                default:
                    Assert.failed("Invalid type");
                }
                return key;
            } catch (Exception x) { 
                throw new StorageError(StorageError.ACCESS_VIOLATION, x);
            }
        }
    }

    static class FullTextSearchableFieldDescriptor { 
        Field field;
        FullTextSearchableFieldDescriptor[] components;

        FullTextSearchableFieldDescriptor(Field f) { 
            field = f;
        }
    }

    boolean isFullTextSearchable() {
        return fullTextSearchableFields.length != 0;
    }

    String className;
    IPersistentSet<CVersionHistory> classExtent;    
    IndexDescriptor[] indices;

    transient FullTextSearchableFieldDescriptor[] fullTextSearchableFields;    
    transient Field[] cloneableFields;
    transient Class   type;
    transient boolean notVersioned;
    transient TableDescriptor supertable;

    public Iterator<CVersionHistory> iterator() { 
        return classExtent.iterator();
    }

    static Field[] buildIndexableList(Class type) 
    {
        ArrayList<Field> fieldList = new ArrayList<Field>();
        for (Field f : type.getDeclaredFields()) { 
            if ((f.getModifiers() & (Modifier.TRANSIENT|Modifier.STATIC)) == 0 && f.getAnnotation(Indexable.class) != null) { 
                try { 
                    f.setAccessible(true);
                } catch (Exception x) {}
                
                fieldList.add(f);
            }
        }        
        Field[] arr = fieldList.toArray(new Field[fieldList.size()]);
        Arrays.sort(arr, new Comparator<Field>() { public int compare(Field f1, Field f2) { return f1.getName().compareTo(f2.getName()); } });
        return arr;
    }

    static FullTextSearchableFieldDescriptor[] buildFullTextSearchableList(Class type) 
    {
        ArrayList<FullTextSearchableFieldDescriptor> fieldList = new ArrayList<FullTextSearchableFieldDescriptor>();
        do { 
            for (Field f : type.getDeclaredFields()) { 
                if ((f.getModifiers() & (Modifier.TRANSIENT|Modifier.STATIC)) == 0 && f.getAnnotation(FullTextSearchable.class) != null) { 
                    try { 
                        f.setAccessible(true);
                    } catch (Exception x) {}
                    
                    FullTextSearchableFieldDescriptor desc = new FullTextSearchableFieldDescriptor(f);
                    Class fieldType = f.getType();
                    if (fieldType != String.class) { 
                        FullTextSearchableFieldDescriptor[] searchableComponents = buildFullTextSearchableList(fieldType);
                        if (searchableComponents.length != 0) { 
                            desc.components = searchableComponents;
                        }
                    }
                    fieldList.add(desc);
                }
            }
        } while ((type = type.getSuperclass()) != null);
        
        return fieldList.toArray(new FullTextSearchableFieldDescriptor[fieldList.size()]);
    }

    static Field[] buildCloneableList(Class type) 
    {
        ArrayList<Field> fieldList = new ArrayList<Field>();
        do { 
            for (Field f : type.getDeclaredFields()) { 
                if ((f.getModifiers() & (Modifier.TRANSIENT|Modifier.STATIC)) == 0) { 
                    Class fieldType = f.getType();
                    if (fieldType == Link.class || ICloneable.class.isAssignableFrom(fieldType) || fieldType.isArray()) { 
                        try { 
                            f.setAccessible(true);
                        } catch (Exception x) {}

                        fieldList.add(f);
                    }
                }
            }
        } while ((type = type.getSuperclass()) != null);
        
        return fieldList.toArray(new Field[fieldList.size()]);
    }

    public void onLoad()
    {
        Storage storage = getStorage();
        type = ClassDescriptor.loadClass(storage, className);
        notVersioned = type.getAnnotation(NotVersioned.class) != null;
        boolean modified = false;

        Field[] indexableFields = buildIndexableList(type);
        IndexDescriptor[] newIndices = new IndexDescriptor[indexableFields.length];
        ArrayList<IndexDescriptor> addedIndices = new ArrayList<IndexDescriptor>();

        for (int i = 0, j = 0; i < indexableFields.length; i++) { 
            String name = indexableFields[i].getName();
            while (j < indices.length && indices[j].fieldName.compareTo(name) < 0) { 
                indices[j].index.deallocate();
                modified = true;
                j += 1;
            }
            IndexDescriptor index;
            Field f = indexableFields[i];
            if (j < indices.length && indices[j].fieldName.equals(name)) { 
                index = indices[j++];
            } else { 
                index = new IndexDescriptor();
                index.index = storage.<VersionHistorySegment>createIndex(f.getType(), false);
                index.fieldName = name;
                index.fieldType = ClassDescriptor.getTypeCode(f.getType());
                addedIndices.add(index);
                modified = true;
            }
            index.field = f;
            Indexable idx = (Indexable)f.getAnnotation(Indexable.class);
            index.unique = idx.unique();
            index.caseInsensitive = idx.caseInsensitive();
            newIndices[i] = index;
        }
        fullTextSearchableFields = buildFullTextSearchableList(type);
        cloneableFields = buildCloneableList(type);
        indices = newIndices;
        if (modified) { 
            store();
            if (addedIndices.size() != 0) { 
                addIndices(addedIndices);
            }
        }
    }
    


    void addIndices(List<IndexDescriptor> addedIndices)
    {
        for (CVersionHistory vh : classExtent) { 
            int last = vh.getNumberOfVersions();
            for (IndexDescriptor desc : addedIndices) { 
                int from = 0;
                CVersion v;
                Key key = null;
                while (true) { 
                    while (++from <= last && ((v = vh.get(from)).isDeleted() || (key = desc.extractKey(v)) == null));
                    if (from <= last) { 
                        int till = from;
                        while (++till <= last && !(v = vh.get(till)).isDeleted() && key.equals(desc.extractKey(v)));
                        desc.index.put(key, new VersionHistorySegment(vh, from, till-1));
                        from = till-1;
                    } else { 
                        break;
                    }
                }
            }
        }
    }
                           

    void cloneFields(CVersion v) 
    {
        try { 
            for (Field f : cloneableFields) {             
                Object o = f.get(v);
                if (o != null) { 
                    if (o instanceof LinkImpl) { 
                        o = new LinkImpl((StorageImpl)getStorage(), (Link)o, v);
                    } else if (o instanceof ICloneable[]) { 
                        ICloneable[] arr = (ICloneable[])((Object[])o).clone();
                        for (int i = 0; i < arr.length; i++) { 
                            arr[i] = (ICloneable)arr[i].clone();
                        }
                    } else if (o instanceof Object[]) { 
                        o = ((Object[])o).clone();
                    } else { 
                        o = ((ICloneable)o).clone();
                    }
                    f.set(v, o);
                }
            }
        } catch (CloneNotSupportedException x) { 
            throw new CloneNotSupportedError();
        } catch (IllegalAccessException x) { 
            throw new IllegalAccessError();
        }
    }

    void excludeFromIndices(CVersion v) 
    {
        TableDescriptor td = this;
        do {             
            for (IndexDescriptor id : td.indices) { 
                for (VersionHistorySegment vhs : id.find(v)) { 
                    if (vhs.vh == v.history && vhs.from == v.id) { 
                        if (vhs.from + 1 == vhs.till) { 
                            id.remove(vhs, v);
                        } else { 
                            vhs.decrement();
                        }
                        break;
                    }
                }
            }
        } while ((td = td.supertable) != null);
    }

    void includeInIndices(CVersion v) 
    { 
        TableDescriptor td = this;
        do {             
          ForEachIndex:
            for (IndexDescriptor id : td.indices) { 
                for (VersionHistorySegment vhs : id.find(v)) { 
                    if (vhs.vh == v.history && vhs.till+1 == v.id) { 
                        vhs.increment();
                        continue ForEachIndex;
                    }
                }
                id.add(v);
            }
        } while ((td = td.supertable) != null);
    }

    void checkConstraints(CVersion v) throws NotUniqueException
    { 
        TableDescriptor td = this;
        do {             
            for (IndexDescriptor id : td.indices) { 
                if (id.unique) { 
                    for (VersionHistorySegment vhs : id.find(v)) { 
                        if (vhs.vh != v.history && vhs.containsLastVersion()) { 
                            throw new NotUniqueException(v);
                        }
                    }
                }
            }
        } while ((td = td.supertable) != null);
    }

    IndexDescriptor findIndex(String name) 
    { 
        TableDescriptor td = this;
        do {             
            for (IndexDescriptor id : td.indices) { 
                if (name.equals(id.fieldName)) { 
                    return id;
                }
            }
        } while ((td = td.supertable) != null);
        return null;
    }

    void registerIndices(Query q, IResource resource, VersionSelector selector)
    {
        TableDescriptor td = this;
        do {             
            for (IndexDescriptor id : td.indices) {
                q.addIndex(id.fieldName, new IndexFilter(id, resource, selector));
            }
        } while ((td = td.supertable) != null);
    }
        
    private String getKeyText(Object val) { 
        return val instanceof Decimal ? ((Decimal)val).toLexicographicString() : val.toString();
    }

    void addDocumentFields(Document doc, Object obj, FullTextSearchableFieldDescriptor[] fullTextSearchableFields, StringBuilder any)
    {
        try { 
            for (FullTextSearchableFieldDescriptor desc : fullTextSearchableFields) {
                Object value = desc.field.get(obj);
                if (value != null) { 
                    if (desc.components != null) { 
                        addDocumentFields(doc, value, desc.components, any);
                    } else if (value instanceof Map) { 
                        for (Map.Entry e : ((Map<?,?>)value).entrySet()) {
                            String text = getKeyText(e.getValue());
                            any.append(text);
                            any.append(' ');
                            doc.add(new org.apache.lucene.document.Field
                                    (e.getKey().toString(), 
                                     text, 
                                     org.apache.lucene.document.Field.Store.YES, 
                                     org.apache.lucene.document.Field.Index.UN_TOKENIZED));
                        }
                    } else { 
                        String text = getKeyText(value);
                        any.append(text);
                        any.append(' ');
                        doc.add(new org.apache.lucene.document.Field
                                (desc.field.getName(), 
                                 text, 
                                 org.apache.lucene.document.Field.Store.YES, 
                                 org.apache.lucene.document.Field.Index.TOKENIZED));
                    }
                }
            }    
        } catch (IllegalAccessException x) { 
            throw new IllegalAccessError();
        }
    }

    Document buildDocument(CVersion v) 
    {
        if (fullTextSearchableFields.length == 0) { 
            return null;
        }
        Document doc = new Document();
        StringBuilder any = new StringBuilder();
        addDocumentFields(doc, v, fullTextSearchableFields, any);
        doc.add(new org.apache.lucene.document.Field
                ("Oid", 
                 Integer.toString(v.getOid()), 
                 org.apache.lucene.document.Field.Store.YES, 
                 org.apache.lucene.document.Field.Index.UN_TOKENIZED));
        doc.add(new org.apache.lucene.document.Field
                ("Class", 
                 v.getClass().getName(),
                 org.apache.lucene.document.Field.Store.YES, 
                 org.apache.lucene.document.Field.Index.UN_TOKENIZED));
        doc.add(new org.apache.lucene.document.Field
                ("Created", 
                 DateTools.timeToString(System.currentTimeMillis(), DateTools.Resolution.MINUTE),
                 org.apache.lucene.document.Field.Store.YES, 
                 org.apache.lucene.document.Field.Index.UN_TOKENIZED));
        doc.add(new org.apache.lucene.document.Field
                ("Any", any.toString(),
                 org.apache.lucene.document.Field.Store.YES, 
                 org.apache.lucene.document.Field.Index.TOKENIZED));
        return doc;
    }

    TableDescriptor(Storage storage, Class table) 
    {
        super(storage);
        type = table;
        notVersioned = type.getAnnotation(NotVersioned.class) != null;
        className = table.getName();
        classExtent = storage.<CVersionHistory>createSet();

        fullTextSearchableFields = buildFullTextSearchableList(table);
        Field[] indexableFields = buildIndexableList(table);
        indices = new IndexDescriptor[indexableFields.length];
        for (int i = 0; i < indexableFields.length; i++) { 
            Field f =  indexableFields[i];
            String name = f.getName();
            //System.out.println("Create index for field " + table.getName () + '.' + name);
            IndexDescriptor index = new IndexDescriptor();
            index.index = storage.<VersionHistorySegment>createIndex(f.getType(), false);
            index.field = f;
            index.fieldName = name;
            index.fieldType = ClassDescriptor.getTypeCode(f.getType());
            Indexable idx = (Indexable)f.getAnnotation(Indexable.class);
            index.unique = idx.unique();
            index.caseInsensitive = idx.caseInsensitive();
            indices[i] = index;
        }
    }
}