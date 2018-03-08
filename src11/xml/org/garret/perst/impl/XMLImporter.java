package org.garret.perst.impl;

import org.garret.perst.*;
import java.io.*;
import java.util.*;

/**
 * Class for importing database from XML dump (which is expected to be created before by XMLExporter
 * or be produced from other data source).
 * This class should be used in this way:
 *     Reader reader = new BufferedReader(new FileReader("dump.xml"));
 *     XMLImporter importer = nw XMLExporter(storage, reader);
 *     importer.importDatabase();
 *     reader.close();
 * Storage should be empty before performing import but application should
 * contain definition of all imported classes.
 */
public class XMLImporter 
{
    /**
     * Constructor of XML importer
     * @param storage destination storage
     * @param reader XML reader
     */
    public XMLImporter(Storage storage, Reader reader) 
    { 
        this.storage = (StorageImpl)storage;
        scanner = new XMLScanner(reader);
    }

    /**
     * Import all data from the XML file in the storage.
     * Storage should be empty before performing import but application should
     * contain definition of all imported classes.
     */
    public void importDatabase() { 
        if (scanner.scan() != XMLScanner.XML_LT
            || scanner.scan() != XMLScanner.XML_IDENT
            || !scanner.getIdentifier().equals("database"))
        { 
            throwException("No root element");
        }    
        if (scanner.scan() != XMLScanner.XML_IDENT
            || !scanner.getIdentifier().equals("root")
            || scanner.scan() != XMLScanner.XML_EQ
            || scanner.scan() != XMLScanner.XML_SCONST 
            || scanner.scan() != XMLScanner.XML_GT) 
        {
            throwException("Database element should have \"root\" attribute");
        }
        int rootId = 0;
        try { 
            rootId = Integer.parseInt(scanner.getString());
        } catch (NumberFormatException x) { 
            throwException("Incorrect root object specification");
        }
        idMap = new int[rootId*2];
        idMap[rootId] = storage.allocateId();
        storage.header.root[1-storage.currIndex].rootObject = idMap[rootId];

        XMLElement elem;
        int tkn;
        while ((tkn = scanner.scan()) == XMLScanner.XML_LT) { 
            if (scanner.scan() != XMLScanner.XML_IDENT) { 
                throwException("Element name expected");
            }
            String elemName = scanner.getIdentifier();
            if (elemName.equals("org.garret.perst.impl.Btree") 
                || elemName.equals("org.garret.perst.impl.BitIndexImpl")
                || elemName.equals("org.garret.perst.impl.PersistentSet") 
                || elemName.equals("org.garret.perst.impl.BtreeCompoundIndex"))
            { 
                createIndex(elemName);
            } else { 
                createObject(readElement(elemName));
            }
        }
        if (tkn != XMLScanner.XML_LTS
            || scanner.scan() != XMLScanner.XML_IDENT
            || !scanner.getIdentifier().equals("database")
            || scanner.scan() != XMLScanner.XML_GT)
        {
            throwException("Root element is not closed");
        }                
    }

    static class XMLElement { 
        private XMLElement next;
        private XMLElement prev;
        private String     name;
        private Hashtable  siblings;
        private Hashtable  attributes;
        private String     svalue;
        private long       ivalue;
        private double     rvalue;
        private int        valueType;
        private int        counter;

        static final int NO_VALUE     = 0;
        static final int STRING_VALUE = 1;
        static final int INT_VALUE    = 2;
        static final int REAL_VALUE   = 3;
        static final int NULL_VALUE   = 4;

        XMLElement(String name) { 
            this.name = name;
            valueType = NO_VALUE;
        }

        final void addSibling(XMLElement elem) { 
            if (siblings == null) { 
                siblings = new Hashtable();
            }
            XMLElement prev = (XMLElement)siblings.get(elem.name);
            if (prev != null) { 
                elem.next = null;
                elem.prev = prev.prev;
                elem.prev.next = elem;
                prev.prev = elem;
                prev.counter += 1;
            } else { 
                siblings.put(elem.name, elem);
                elem.prev = elem;
                elem.counter = 1;
            }
        }

        final void addAttribute(String name, String value) { 
            if (attributes == null) { 
                attributes = new Hashtable();
            }
            attributes.put(name, value);
        }

        final XMLElement getSibling(String name) { 
            if (siblings != null) { 
                return (XMLElement)siblings.get(name);
            }
            return null;
        }

        final static Enumeration EMPTY_ENUMERATION = (new Vector()).elements();
        
        final Enumeration getSiblings() { 
            return (siblings != null) ? siblings.elements() : EMPTY_ENUMERATION;
        } 

        final XMLElement getFirstSibling() { 
            Enumeration e = getSiblings();
            return e.hasMoreElements() ? (XMLElement)e.nextElement() : null;
        }

        final XMLElement getNextSibling() { 
            return next;
        }
        
        final String getName() {
            return name;
        }

        final int getCounter() { 
            return counter;
        }

        final String getAttribute(String name) { 
            return attributes != null ? (String)attributes.get(name) : null;
        }

        final void setIntValue(long val) { 
            ivalue = val;
            valueType = INT_VALUE;
        }
        
        final void setRealValue(double val) { 
            rvalue = val;
            valueType = REAL_VALUE;
        }
        
        final void setStringValue(String val) { 
            svalue = val;
            valueType = STRING_VALUE;
        }

        final void setNullValue() { 
            valueType = NULL_VALUE;
        }
           
    
        final String getStringValue() { 
            return svalue;
        }

        final long getIntValue() { 
            return ivalue;
        }

        final double getRealValue() { 
            return rvalue;
        }

        final boolean isIntValue() { 
            return valueType == INT_VALUE;
        }
    
        final boolean isRealValue() { 
            return valueType == REAL_VALUE;
        }

        final boolean isStringValue() { 
            return valueType == STRING_VALUE;
        }
    
        final boolean isNullValue() { 
            return valueType == NULL_VALUE;
        }
    }        

    final String getAttribute(XMLElement elem, String name)
    { 
        String value = elem.getAttribute(name);
        if (value == null) { 
            throwException("Attribute " + name + " is not set");
        }
        return value;
    }


    final int getIntAttribute(XMLElement elem, String name)
    { 
        String value = elem.getAttribute(name);
        if (value == null) { 
            throwException("Attribute " + name + " is not set");
        }
        try { 
            return Integer.parseInt(value);
        } catch (NumberFormatException x) { 
            throwException("Attribute " + name + " should has integer value");
        }
        return -1;
    }

    final int mapId(int id) 
    {
        int oid = 0;
        if (id != 0) {
            if (id >= idMap.length) { 
                int[] newMap = new int[id*2];
                System.arraycopy(idMap, 0, newMap, 0, idMap.length);
                idMap = newMap;
                idMap[id] = oid = storage.allocateId();
            } else { 
                oid = idMap[id];
                if (oid == 0) { 
                    idMap[id] = oid = storage.allocateId();
                }
            }
        }
        return oid;
    }

    final int getType(String value) { 
        return Integer.parseInt(value);
    }

    final Key createCompoundKey(int[] types, String[] values)
    {
        Object obj;
        Date date;
        ByteBuffer buf = new ByteBuffer();
        int dst = 0;

        try { 
            for (int i = 0; i < types.length; i++) { 
                String value = values[i];
                switch (types[i]) { 
                  case Types.Boolean:
                    buf.extend(dst+1);
                    buf.arr[dst++] = (byte)(Integer.parseInt(value) != 0 ? 1 : 0);
                    break;
                  case Types.Byte:
                    buf.extend(dst+1);
                    buf.arr[dst++] = Byte.parseByte(value);
                    break;
                  case Types.Char:
                    buf.extend(dst+2);
                    Bytes.pack2(buf.arr, dst, (short)Integer.parseInt(value));
                    dst += 2;
                    break;
                  case Types.Short:
                    buf.extend(dst+2);
                    Bytes.pack2(buf.arr, dst, Short.parseShort(value));
                    dst += 2;
                    break;
                  case Types.Int:
                    buf.extend(dst+4);
                    Bytes.pack4(buf.arr, dst, Integer.parseInt(value));
                    dst += 4;
                    break;
                  case Types.Object:
                    buf.extend(dst+4);
                    Bytes.pack4(buf.arr, dst, mapId(Integer.parseInt(value)));
                    dst += 4;
                    break;
                  case Types.Long:
                  case Types.Date:
                    buf.extend(dst+8);
                    Bytes.pack8(buf.arr, dst, Long.parseLong(value));
                    dst += 8;
                    break;
                  case Types.Float:
                    buf.extend(dst+4);
                    Bytes.pack4(buf.arr, dst, Float.floatToIntBits(Float.valueOf(value).floatValue()));
                    dst += 4;
                    break;
                  case Types.Double:
                    buf.extend(dst+8);
                    Bytes.pack8(buf.arr, dst, Double.doubleToLongBits(Double.valueOf(value).doubleValue()));
                    dst += 8;
                    break;
                  case Types.String:
                    buf.extend(dst + 2 + (value.length()*2));
                    Bytes.pack2(buf.arr, dst, (short)value.length());
                    dst += 2;
                    for (int j = 0, n = value.length(); j < n; j++) { 
                        Bytes.pack2(buf.arr, dst, (short)value.charAt(j));
                        dst += 2;
                    }
                    break;
                  case Types.ArrayOfByte:
                    buf.extend(dst + 4 + (value.length() >>> 1));
                    Bytes.pack4(buf.arr, dst, value.length() >>> 1);
                    dst += 4;
                    for (int j = 0, n = value.length(); j < n; j+=2) { 
                        buf.arr[dst++] = (byte)((getHexValue(value.charAt(j)) << 4) 
                                                | getHexValue(value.charAt(j+1)));
                    }
                    break;
                  default:
                    throwException("Bad key type");
                }
            }
        } catch (NumberFormatException x) { 
            throwException("Failed to convert key value");
        }
        return new Key(buf.toArray());
    }

    final static String months[] = { "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec" };

    final Date parseDate(String value) 
    { 
        try { 
            int month;
            Calendar calendar = Calendar.getInstance();
            String monthName = value.substring(4, 7);
            for (month = 0; !months[month].equals(monthName); month++);
            calendar.set(Calendar.MONTH, month);
            calendar.set(Calendar.DAY_OF_MONTH, Integer.parseInt(value.substring(8, 10)));
            calendar.set(Calendar.HOUR_OF_DAY, Integer.parseInt(value.substring(11, 13)));
            calendar.set(Calendar.MINUTE, Integer.parseInt(value.substring(14, 16)));
            calendar.set(Calendar.SECOND, Integer.parseInt(value.substring(17, 19)));
            calendar.set(Calendar.MILLISECOND, 0);
            //TimeZone timeZone = TimeZone.getTimeZone(value.substring(20, value.length()-5));
            //calendar.setTimeZone(timeZone);
            calendar.set(Calendar.YEAR, Integer.parseInt(value.substring(value.length()-4)));
            return calendar.getTime();
        } catch (Exception x) { 
            throwException("Bad date constant");
        }
        return null;
    }

    final Key createKey(int type, String value)
    { 
        try { 
            switch (type) { 
                case Types.Boolean:
                    return new Key(Integer.parseInt(value) != 0);
                 case Types.Byte:
                    return new Key(Byte.parseByte(value));
                case Types.Char:
                    return new Key((char)Integer.parseInt(value));
                case Types.Short:
                    return new Key(Short.parseShort(value));
                case Types.Int:
                    return new Key(Integer.parseInt(value));
                case Types.Object:
                    return new Key(new PersistentStub(storage, mapId(Integer.parseInt(value))));
                case Types.Long:
                    return new Key(Long.parseLong(value));
                case Types.Float:
                    return new Key(Float.valueOf(value).floatValue());
                case Types.Double:
                    return new Key(Double.valueOf(value).doubleValue());
                case Types.String:
                    return new Key(value);
                case Types.ArrayOfByte:
                {
                    byte[] buf = new byte[value.length() >> 1];
                    for (int i = 0; i < buf.length; i++) { 
                        buf[i] = (byte)((getHexValue(value.charAt(i*2)) << 4) | getHexValue(value.charAt(i*2+1)));
                    }
                    return new Key(buf);
                }
                case Types.Date:
                {
                    Date date = null;
                    if (!value.equals("null")) {
                        date = parseDate(value);
                    }
                    return new Key(date);
                }
                default:
                    throwException("Bad key type");
            }
        } catch (NumberFormatException x) { 
            throwException("Failed to convert key value");
        }
        return null;
    }

    final int parseInt(String str)
    {
        try { 
            return Integer.parseInt(str);
        } catch (NumberFormatException x) { 
            throwException("Bad integer constant");
        }
        return -1;
    }

    Class findClass(String name) { 
        if (name == null) { 
            throwException("class not specified");
        }
        try { 
            return Class.forName(name.replace('-', '$'));
        } catch (ClassNotFoundException x) { 
            throwException("class " + name + " not found");
        }                
        return null;
    }
    
    IPersistent createInstance(Class cls) { 
        try { 
            return (IPersistent)cls.newInstance();
        } catch (Exception x) { 
            throwException("failed to create instance of class " + cls + ": " + x);
        }        
        return null;
    }            

    IPersistent importRef(XMLElement elem)
    {
        IPersistent obj = null;
        if (elem != null && !elem.isNullValue()) { 
            XMLElement ref = elem.getSibling("ref");
            if (ref == null) { 
                throwException("object reference expected");
            }
            int oid = mapId(getIntAttribute(ref, "id"));
            obj = createInstance(findClass(ref.getAttribute("type")));
            obj.assignOid(storage, oid, false);
        }
        return obj;
    }

    final void createIndex(String indexType)
    {
        Btree btree = null;
        int tkn;
        int oid = 0;
        boolean unique = false;
        int[] types = null;
        String type = null;
        while ((tkn = scanner.scan()) == XMLScanner.XML_IDENT) { 
            String attrName = scanner.getIdentifier();
            if (scanner.scan() != XMLScanner.XML_EQ || scanner.scan() != XMLScanner.XML_SCONST) {
                throwException("Attribute value expected");
            }
            String attrValue = scanner.getString();
            if (attrName.equals("id")) { 
                oid = mapId(parseInt(attrValue));
            } else if (attrName.equals("unique")) { 
                unique = parseInt(attrValue) != 0;
            } else if (attrName.equals("type")) { 
                type = attrValue;
            } else if (attrName.startsWith("type")) {
                int typeNo = Integer.parseInt(attrName.substring(4));
                if (types == null || types.length <= typeNo) { 
                    int[] newTypes = new int[typeNo+1];
                    if (types != null) { 
                        System.arraycopy(types, 0, newTypes, 0, types.length);
                    }
                    types = newTypes;
                }
                types[typeNo] = getType(attrValue);
            }
        }
        if (tkn != XMLScanner.XML_GT) { 
            throwException("Unclosed element tag");
        }
        if (oid == 0) { 
            throwException("ID is not specified or index");
        }
        if (types != null) { 
            btree = new BtreeCompoundIndex(types, unique);
        } else if (type == null) { 
            if (indexType.equals("org.garret.perst.impl.PersistentSet")) { 
                btree = new PersistentSet();
            } else { 
                throwException("Key type is not specified for index");
            }
        } else { 
            if (indexType.equals("org.garret.perst.impl.BitIndexImpl")) { 
                btree = new BitIndexImpl();
            } else { 
                btree = new Btree(getType(type), unique);
            }
        }
        btree.assignOid(storage, oid, false);
        storage.objectCache.put(oid, btree);
        while ((tkn = scanner.scan()) == XMLScanner.XML_LT) {
            if (scanner.scan() != XMLScanner.XML_IDENT
                || !scanner.getIdentifier().equals("ref"))
            {
                throwException("<ref> element expected");
            }   
            XMLElement ref = readElement("ref");
            Key key = null;
            int mask = 0;
            if (types != null) { 
                String[] values = new String[types.length];                
                for (int i = 0; i < values.length; i++) { 
                    values[i] = getAttribute(ref, "key"+i);
                }
                key = createCompoundKey(types, values);
            } else { 
                if (btree instanceof BitIndex) { 
                    mask = getIntAttribute(ref, "key");
                } else { 
                    key = createKey(btree.type, getAttribute(ref, "key"));
                }
            }
            IPersistent obj = new PersistentStub(storage, mapId(getIntAttribute(ref, "id")));
            if (btree instanceof BitIndex) { 
                ((BitIndex)btree).put(obj, mask);
            } else { 
                btree.insert(key, obj, false);
            } 
        }
        if (tkn != XMLScanner.XML_LTS 
            || scanner.scan() != XMLScanner.XML_IDENT
            || !scanner.getIdentifier().equals(indexType)
            || scanner.scan() != XMLScanner.XML_GT)
        {
            throwException("Element is not closed");
        }     
        btree.store();
    }

    final void createObject(XMLElement elem)
    {
        IPersistent obj = createInstance(findClass(elem.name));
        int oid = mapId(getIntAttribute(elem, "id"));
        obj.readObject(new XMLInputStream(elem));
        obj.assignOid(storage, oid, false);
        obj.store();
        obj.invalidate();
    }

    final int getHexValue(char ch)
    { 
        if (ch >= '0' && ch <= '9') { 
            return ch - '0';
        } else if (ch >= 'A' && ch <= 'F') { 
            return ch - 'A' + 10;
        } else if (ch >= 'a' && ch <= 'f') { 
            return ch - 'a' + 10;
        } else { 
            throwException("Bad hexadecimal constant");
        }
        return -1;
    }

    class XMLInputStream implements IInputStream { 
        XMLElement currColumn;
        int        columnNo;
        int        nestingLevel;

        XMLInputStream(XMLElement row) { 
            currColumn = row.getSibling(nestingLevel != 0 ? "object" : "column");
            columnNo = 0;
        }
            
        public InputStream getInputStream() { 
            return null;
        }

        private final XMLElement getColumn() { 
            if (currColumn == null) { 
                return null;
            }
            XMLElement column = currColumn;
            if (nestingLevel == 0 && ++columnNo != getIntAttribute(column, "no")) {
                throwException("Unexpected column number");
            }
            currColumn = column.getNextSibling();
            return column;
        }

        public boolean end()
        {
            return currColumn == null;
        }
        
        public boolean readBoolean() { 
            XMLElement column = getColumn();
            if (column != null) { 
                if (column.isIntValue()) {
                    return column.getIntValue() != 0;
                } else if (column.isRealValue()) {
                    return column.getRealValue() != 0.0;
                } else { 
                    throwException("Conversion for column " + columnNo + " is not possible");
                }
            }
            return false;
        }

        public byte readByte() {  
            XMLElement column = getColumn();
            if (column != null) { 
                if (column.isIntValue()) {
                    return (byte)column.getIntValue();
                } else if (column.isRealValue()) {
                    return (byte)column.getRealValue();
                } else { 
                    throwException("Conversion for column " + columnNo + " is not possible");
                }
            }
            return (byte)0;
        }

        public char readChar() {
            XMLElement column = getColumn();
            if (column != null) { 
                if (column.isIntValue()) {
                    return (char)column.getIntValue();
                } else if (column.isRealValue()) {
                    return (char)column.getRealValue();
                } else { 
                    throwException("Conversion for column " + columnNo + " is not possible");
                }
            }
            return (char)0;
        }

        public short readShort() {
            XMLElement column = getColumn();
            if (column != null) { 
                if (column.isIntValue()) {
                    return (short)column.getIntValue();
                } else if (column.isRealValue()) {
                    return (short)column.getRealValue();
                } else { 
                    throwException("Conversion for column " + columnNo + " is not possible");
                }
            }
            return (short)0;
        }

        public int readInt() { 
            XMLElement column = getColumn();
            if (column != null) { 
                if (column.isIntValue()) {
                    return (int)column.getIntValue();
                } else if (column.isRealValue()) {
                    return (int)column.getRealValue();
                } else { 
                    throwException("Conversion for column " + columnNo + " is not possible");
                }
            }
            return 0;
        }
        
        public long readLong() { 
            XMLElement column = getColumn();
            if (column != null) { 
                if (column.isIntValue()) {
                    return column.getIntValue();
                } else if (column.isRealValue()) {
                    return (long)column.getRealValue();
                } else { 
                    throwException("Conversion for column " + columnNo + " is not possible");
                }
            }
            return 0;
        }

        public float readFloat() { 
            XMLElement column = getColumn();
            if (column != null) { 
                if (column.isIntValue()) {
                    return (float)column.getIntValue();
                } else if (column.isRealValue()) {
                    return (float)column.getRealValue();
                } else { 
                    throwException("Conversion for column " + columnNo + " is not possible");
                }
            }
            return (float)0;
        }
        
        public double readDouble() { 
            XMLElement column = getColumn();
            if (column != null) { 
                if (column.isIntValue()) {
                    return (double)column.getIntValue();
                } else if (column.isRealValue()) {
                    return (double)column.getRealValue();
                } else { 
                    throwException("Conversion for column " + columnNo + " is not possible");
                }
            }
            return 0.0;
        }

        public String readString() { 
            XMLElement column = getColumn();
            String value = null;
            if (column != null && !column.isNullValue()) { 
                if (column.isIntValue()) {
                    value = Long.toString(column.getIntValue());
                } else if (column.isRealValue()) {
                    value = Double.toString(column.getRealValue());
                } else if (column.isStringValue()) {
                    value = column.getStringValue();
                } else { 
                    throwException("Conversion for column " + columnNo + " is not possible");
                }
            }
            return value;
        }

        public Date readDate() { 
            XMLElement column = getColumn();
            if (column != null && !column.isNullValue()) { 
                if (column.isIntValue()) {
                    return new Date(column.getIntValue());
                } else if (column.isStringValue()) {
                    return parseDate(column.getStringValue());
                } else if (column.isRealValue()) {
                    return new Date((long)(column.getRealValue()*1000));
                } else { 
                    throwException("Conversion for column " + columnNo + " is not possible");
                }
            }
            return null;
        }

        public IPersistent readObject() { 
            XMLElement column = getColumn();
            if (column != null && !column.isNullValue()) { 
                return importRef(column);
            }
            return null;
        }
        
        public Link readLink() {
            XMLElement column = getColumn();
            if (column != null && !column.isNullValue()) { 
                XMLElement item = column.getSibling("element");
                int len = (item == null) ? 0 : item.getCounter(); 
                Link link = storage.createLink(len);
                while (--len >= 0) { 
                    IPersistent elem = null;
                    if (item != null && !item.isNullValue()) { 
                        XMLElement ref = item.getSibling("ref");
                        if (ref == null) { 
                            throwException("object reference expected");
                        }
                        int oid = mapId(getIntAttribute(ref, "id"));
                        elem = new PersistentStub(storage, oid);
                    }
                    link.add(elem);
                    item = item.getNextSibling();
                }
                return link;
            }
            return null;
        }

        public  boolean[] readArrayOfBoolean() { 
            XMLElement column = getColumn();
            if (column != null && !column.isNullValue()) { 
                XMLElement item = column.getSibling("element");
                int len = (item == null) ? 0 : item.getCounter(); 
                boolean[] arr = new boolean[len];
                for (int i = 0; i < len; i++) { 
                    if (item.isIntValue()) { 
                        arr[i] = item.getIntValue() != 0;
                    } else if (item.isRealValue()) { 
                        arr[i] = item.getRealValue() != 0.0;
                    } else {
                        throwException("Conversion for column " + columnNo + " is not possible");
                    }
                    item = item.getNextSibling();
                }
                return arr;
            }
            return null;
        }
        
        public byte[] readArrayOfByte() {
            XMLElement column = getColumn();
            byte[] arr = null;
            if (column != null) {
                if (column.isStringValue()) {
                    String hexStr = column.getStringValue();
                    int len = hexStr.length();
                    arr = new byte[len/2];
                    for (int j = 0; j < arr.length; j++) { 
                        arr[j] = (byte)((getHexValue(hexStr.charAt(j*2)) << 4) | getHexValue(hexStr.charAt(j*2+1)));
                    }
                } else if (!column.isNullValue()) { 
                    XMLElement item = column.getSibling("element");
                    int len = (item == null) ? 0 : item.getCounter(); 
                    arr = new byte[len];
                    for (int i = 0; i < len; i++) { 
                        if (item.isIntValue()) { 
                            arr[i] = (byte)item.getIntValue();
                        } else if (item.isRealValue()) { 
                            arr[i] = (byte)item.getRealValue();
                        } else {
                            throwException("Conversion for column " + columnNo + " is not possible");
                        }
                        item = item.getNextSibling();
                    }
                } else { 
                    throwException("Conversion for column " + columnNo + " is not possible");
                }
            }
            return arr;
        }
        
        public char[] readArrayOfChar() {
            XMLElement column = getColumn();
            if (column != null && !column.isNullValue()) { 
                XMLElement item = column.getSibling("element");
                int len = (item == null) ? 0 : item.getCounter(); 
                char[] arr = new char[len];
                for (int i = 0; i < len; i++) { 
                    if (item.isIntValue()) { 
                        arr[i] = (char)item.getIntValue();
                    } else if (item.isRealValue()) { 
                        arr[i] = (char)item.getRealValue();
                    } else {
                        throwException("Conversion for column " + columnNo + " is not possible");
                    }
                    item = item.getNextSibling();
                }
                return arr;
            }
            return null;
        }
        
        public short[] readArrayOfShort() { 
            XMLElement column = getColumn();
            if (column != null && !column.isNullValue()) { 
                XMLElement item = column.getSibling("element");
                int len = (item == null) ? 0 : item.getCounter(); 
                short[] arr = new short[len];
                for (int i = 0; i < len; i++) { 
                    if (item.isIntValue()) { 
                        arr[i] = (short)item.getIntValue();
                    } else if (item.isRealValue()) { 
                        arr[i] = (short)item.getRealValue();
                    } else {
                        throwException("Conversion for column " + columnNo + " is not possible");
                    }
                    item = item.getNextSibling();
                }
                return arr;
            }
            return null;
        }
        
        public int[] readArrayOfInt() {
            XMLElement column = getColumn();
            if (column != null && !column.isNullValue()) { 
                XMLElement item = column.getSibling("element");
                int len = (item == null) ? 0 : item.getCounter(); 
                int[] arr = new int[len];
                for (int i = 0; i < len; i++) { 
                    if (item.isIntValue()) { 
                        arr[i] = (int)item.getIntValue();
                    } else if (item.isRealValue()) { 
                        arr[i] = (int)item.getRealValue();
                    } else {
                        throwException("Conversion for column " + columnNo + " is not possible");
                    }
                    item = item.getNextSibling();
                }
                return arr;
            }
            return null;
        }
        
        public long[] readArrayOfLong() { 
            XMLElement column = getColumn();
            if (column != null && !column.isNullValue()) { 
                XMLElement item = column.getSibling("element");
                int len = (item == null) ? 0 : item.getCounter(); 
                long[] arr = new long[len];
                for (int i = 0; i < len; i++) { 
                    if (item.isIntValue()) { 
                        arr[i] = item.getIntValue();
                    } else if (item.isRealValue()) { 
                        arr[i] = (long)item.getRealValue();
                    } else {
                        throwException("Conversion for column " + columnNo + " is not possible");
                    }
                    item = item.getNextSibling();
                }
                return arr;
            }
            return null;
        }
        
        public float[] readArrayOfFloat() {
            XMLElement column = getColumn();
            if (column != null && !column.isNullValue()) { 
                XMLElement item = column.getSibling("element");
                int len = (item == null) ? 0 : item.getCounter(); 
                float[] arr = new float[len];
                for (int i = 0; i < len; i++) { 
                    if (item.isIntValue()) { 
                        arr[i] = (float)item.getIntValue();
                    } else if (item.isRealValue()) { 
                        arr[i] = (float)item.getRealValue();
                    } else {
                        throwException("Conversion for column " + columnNo + " is not possible");
                    }
                    item = item.getNextSibling();
                }
                return arr;
            }
            return null;
        }
        
        public double[] readArrayOfDouble() { 
            XMLElement column = getColumn();
            if (column != null && !column.isNullValue()) { 
                XMLElement item = column.getSibling("element");
                int len = (item == null) ? 0 : item.getCounter(); 
                double[] arr = new double[len];
                for (int i = 0; i < len; i++) { 
                    if (item.isIntValue()) { 
                        arr[i] = (double)item.getIntValue();
                    } else if (item.isRealValue()) { 
                        arr[i] = item.getRealValue();
                    } else {
                        throwException("Conversion for column " + columnNo + " is not possible");
                    }
                    item = item.getNextSibling();
                }
                return arr;
            }
            return null;
        }
        
        public String[] readArrayOfString() { 
            XMLElement column = getColumn();
            if (column != null && !column.isNullValue()) { 
                XMLElement item = column.getSibling("element");
                int len = (item == null) ? 0 : item.getCounter(); 
                String[] arr = new String[len];
                for (int i = 0; i < len; i++) { 
                    String value = null;
                    if (item.isIntValue()) {
                        value = Long.toString(item.getIntValue());
                    } else if (item.isRealValue()) {
                        value = Double.toString(item.getRealValue());
                    } else if (item.isStringValue()) {
                        value = item.getStringValue();
                    } else if (!item.isNullValue()) {
                        throwException("Conversion for column " + columnNo + " is not possible");
                    }
                    arr[i] = value;
                    item = item.getNextSibling();
                }
                return arr;
            }
            return null;
        }
 
        public Date[] readArrayOfDate() { 
            XMLElement column = getColumn();
            if (column != null && !column.isNullValue()) { 
                XMLElement item = column.getSibling("element");
                int len = (item == null) ? 0 : item.getCounter(); 
                Date[] arr = new Date[len];
                for (int i = 0; i < len; i++) { 
                    Date value = null;
                    if (item.isStringValue()) { 
                        value = parseDate(item.getStringValue());
                    } else if (item.isIntValue()) {
                        value = new Date(item.getIntValue());
                    } else if (item.isRealValue()) {
                        value = new Date((long)(item.getRealValue()*1000));
                    } else if (!item.isNullValue()) {
                        throwException("Conversion for column " + columnNo + " is not possible");
                    }
                    arr[i] = value;
                    item = item.getNextSibling();
                }
                return arr;
            }
            return null;
        }
 
        public int readArrayOfObject(IPersistent[] v) { 
            XMLElement column = getColumn();
            if (column != null && !column.isNullValue()) { 
                XMLElement item = column.getSibling("element");
                int len = (item == null) ? 0 : item.getCounter(); 
                if (len > v.length) { 
                    throw new IllegalArgumentException();
                }
                for (int i = 0; i < len; i++) { 
                    v[i] = importRef(item);
                    item = item.getNextSibling();
                }
                return len;
            }
            return -1;
        }

            
        public IPersistent[] readArrayOfObject() { 
            XMLElement column = getColumn();
            if (column != null && !column.isNullValue()) { 
                XMLElement item = column.getSibling("element");
                int len = (item == null) ? 0 : item.getCounter(); 
                IPersistent[] arr = new IPersistent[len];
                for (int i = 0; i < len; i++) { 
                    arr[i] = importRef(item);
                    item = item.getNextSibling();
                }
                return arr;
            }
            return null;
        }

        public Vector readVector() { 
            XMLElement column = getColumn();
            if (column != null && !column.isNullValue()) { 
                XMLElement item = column.getSibling("element");
                int len = (item == null) ? 0 : item.getCounter(); 
                Vector v = new Vector(len);
                nestingLevel += 1;
                for (int i = 0; i < len; i++) { 
                    v.addElement(read(item));
                    item = item.getNextSibling();
                }
                nestingLevel -= 1;
                return v;
            }
            return null;
        }
            
        public Hashtable readHashtable() { 
            XMLElement column = getColumn();
            if (column != null && !column.isNullValue()) { 
                XMLElement entry = column.getSibling("entry");
                int len = (entry == null) ? 0 : entry.getCounter(); 
                Hashtable v = new Hashtable(len);
                nestingLevel += 1;
                for (int i = 0; i < len; i++) { 
                    Object key = read(entry.getSibling("key"));                    
                    Object value = read(entry.getSibling("value"));
                    v.put(key, value);
                    entry = entry.getNextSibling();
                }
                nestingLevel -= 1;
                return v;
            }
            return null;
        }

        private final Object read(XMLElement node) { 
            XMLElement parent = currColumn;
            currColumn = node;
            Object obj = read();
            currColumn = parent;
            return obj;
        }

        public Object read() { 
            XMLElement saveColumn = currColumn;
            int saveColumnNo = columnNo;
            XMLElement column = getColumn();
            if (column != null && !column.isNullValue()) { 
                XMLElement nextColumn = currColumn;
                int nextColumnNo = columnNo;
                columnNo = saveColumnNo;
                currColumn = saveColumn;
                switch (getIntAttribute(column, "type")) { 
                case Types.Boolean:
                    return new Boolean(readBoolean());
                case Types.Byte:
                    return new Byte(readByte());
                case Types.Char:
                    return new Character(readChar());
                case Types.Short:
                    return new Short(readShort());
                case Types.Int:
                    return new Integer(readInt());
                case Types.Long:
                    return new Long(readLong());
                case Types.Float:
                    return new Float(readFloat());
                case Types.Double:
                    return new Double(readDouble());
                case Types.String:
                case Types.AsciiString:
                case Types.ShortAsciiString:
                    return readString();
                case Types.Date:
                    return readDate();
                case Types.Object:
                    return readObject();
                case Types.Link:
                    return readLink();
                case Types.ArrayOfBoolean:
                    return readArrayOfBoolean();
                case Types.ArrayOfByte:
                    return readArrayOfByte();
                case Types.ArrayOfChar:
                    return readArrayOfChar();
                case Types.ArrayOfShort:
                    return readArrayOfShort();
                case Types.ArrayOfInt:
                    return readArrayOfInt();
                case Types.ArrayOfLong:
                    return readArrayOfLong();
                case Types.ArrayOfFloat:
                    return readArrayOfFloat();
                case Types.ArrayOfDouble:
                    return readArrayOfDouble();
                case Types.ArrayOfString:
                    return readArrayOfString();
                case Types.ArrayOfDate:
                    return readArrayOfDate();
                case Types.ArrayOfObject:
                    return readArrayOfObject();
                case Types.Null:
                    currColumn = nextColumn;
                    columnNo = nextColumnNo;
                    return null;
                case Types.Vector:
                    return readVector();
                case Types.Hashtable:
                    return readHashtable();
                default:
                    throw new StorageError(StorageError.UNSUPPORTED_TYPE);
                }
            }
            return null;
        }
    }

    final XMLElement readElement(String name) 
    {
        XMLElement elem = new XMLElement(name);
        String attribute;
        int tkn;
        while (true) { 
            switch (scanner.scan()) { 
              case XMLScanner.XML_GTS:
                return elem;
              case XMLScanner.XML_GT:    
                while ((tkn = scanner.scan()) == XMLScanner.XML_LT) { 
                    if (scanner.scan() != XMLScanner.XML_IDENT) { 
                        throwException("Element name expected");
                    }
                    String siblingName = scanner.getIdentifier();
                    XMLElement sibling = readElement(siblingName);
                    elem.addSibling(sibling);
                }
                switch (tkn) { 
                  case XMLScanner.XML_SCONST:
                    elem.setStringValue(scanner.getString());
                    tkn = scanner.scan();
                    break;
                  case XMLScanner.XML_ICONST:
                    elem.setIntValue(scanner.getInt());
                    tkn = scanner.scan();
                    break;
                  case XMLScanner.XML_FCONST:
                    elem.setRealValue(scanner.getReal());
                    tkn = scanner.scan();
                    break;
                  case XMLScanner.XML_IDENT:
                    if (scanner.getIdentifier().equals("null")) { 
                        elem.setNullValue();
                    } else { 
                        elem.setStringValue(scanner.getIdentifier());
                    }
                    tkn = scanner.scan();
                }
                if (tkn != XMLScanner.XML_LTS                    
                    || scanner.scan() != XMLScanner.XML_IDENT
                    || !scanner.getIdentifier().equals(name)
                    || scanner.scan() != XMLScanner.XML_GT)
                {
                    throwException("Element is not closed");
                }
                return elem;
              case XMLScanner.XML_IDENT:
                attribute = scanner.getIdentifier();
                if (scanner.scan() != XMLScanner.XML_EQ || scanner.scan() != XMLScanner.XML_SCONST)
                {
                    throwException("Attribute value expected");
                }
                elem.addAttribute(attribute, scanner.getString());
                continue;
              default:
                throwException("Unexpected token");
            }
        }
    }

    final void throwException(String message) { 
        throw new XMLImportException(scanner.getLine(), scanner.getColumn(), message);
    }

    StorageImpl storage;
    XMLScanner  scanner;
    int[]       idMap;

    static class XMLScanner { 
        static final int XML_IDENT  = 0;
        static final int XML_SCONST = 1;
        static final int XML_ICONST = 2; 
        static final int XML_FCONST = 3; 
        static final int XML_LT     = 4; 
        static final int XML_GT     = 5; 
        static final int XML_LTS    = 6; 
        static final int XML_GTS    = 7;
        static final int XML_EQ     = 8; 
        static final int XML_EOF    = 9;
        
        Reader         reader;
        int            line;
        int            column;
        char[]         sconst;
        long           iconst;
        double         fconst;
        int            slen;
        String         ident;
        int            size;
        int            ungetChar;
        boolean        hasUngetChar;

        XMLScanner(Reader in) {
            reader = in;
            sconst = new char[size = 1024];
            line = 1;
            column = 0;
            hasUngetChar = false;
        }

        final int get() { 
            if (hasUngetChar) { 
                hasUngetChar = false;
                return ungetChar;
            }
            try { 
                int ch = reader.read();
                if (ch == '\n') { 
                    line += 1;
                    column = 0;
                } else if (ch == '\t') { 
                    column += (column + 8) & ~7;
                } else { 
                    column += 1;
                }
                return ch;
            } catch (IOException x) { 
                throw new XMLImportException(line, column, x.getMessage());
            }
        }
        
        final void unget(int ch) { 
            if (ch == '\n') {
                line -= 1;
            } else { 
                column -= 1;
            }
            ungetChar = ch;
            hasUngetChar = true;
        }

        static boolean isLetterOrDigit(char ch) {
            return (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z');
        }
        
        final int scan()
        {
            int i, ch, quote;
            boolean floatingPoint;

            while (true) { 
                do {
                    if ((ch = get()) < 0) {
                        return XML_EOF;
                    }
                } while (ch <= ' ');
                
                switch (ch) { 
                  case '<':
                    ch = get();
                    if (ch == '?') { 
                        while ((ch = get()) != '?') { 
                            if (ch < 0) { 
                                throw new XMLImportException(line, column, "Bad XML file format");
                            }
                        }
                        if ((ch = get()) != '>') { 
                            throw new XMLImportException(line, column, "Bad XML file format");
                        }
                        continue;
                    } 
                    if (ch != '/') { 
                        unget(ch);
                        return XML_LT;
                    }
                    return XML_LTS;
                  case '>':
                    return XML_GT;
                  case '/':
                    ch = get();
                    if (ch != '>') { 
                        unget(ch);
                        throw new XMLImportException(line, column, "Bad XML file format");
                    }
                    return XML_GTS;
                  case '=':
                    return XML_EQ;
                  case '"':
                  case '\'':
                    quote = ch;
                    i = 0;
                    while (true) { 
                        ch = get();
                        if (ch < 0) { 
                            throw new XMLImportException(line, column, "Bad XML file format");
                        } else if (ch == '&') { 
                            switch (get()) { 
                              case 'a':
                                ch = get();
                                if (ch == 'm') { 
                                    if (get() == 'p' && get() == ';') { 
                                        ch = '&';
                                        break;
                                    }
                                } else if (ch == 'p' && get() == 'o' && get() == 's' && get() == ';') { 
                                    ch = '\'';
                                    break;
                                }
                                throw new XMLImportException(line, column, "Bad XML file format");
                              case 'l':
                                if (get() != 't' || get() != ';') { 
                                    throw new XMLImportException(line, column, "Bad XML file format");
                                }
                                ch = '<';
                                break;
                              case 'g':
                                if (get() != 't' || get() != ';') { 
                                    throw new XMLImportException(line, column, "Bad XML file format");
                                }
                                ch = '>';
                                break;
                              case 'q':
                                if (get() != 'u' || get() != 'o' || get() != 't' || get() != ';') { 
                                    throw new XMLImportException(line, column, "Bad XML file format");
                                }
                                ch = '"';
                                break;
                              default: 
                                throw new XMLImportException(line, column, "Bad XML file format");
                            }
                        } else if (ch == quote) { 
                            slen = i;
                            return XML_SCONST;
                        }
                        if (i == size) { 
                            char[] newBuf = new char[size *= 2];
                            System.arraycopy(sconst, 0, newBuf, 0, i);
                            sconst = newBuf;
                        }
                        sconst[i++] = (char)ch;
                    }
                  case '-': case '+':
                  case '0': case '1': case '2': case '3': case '4':
                  case '5': case '6': case '7': case '8': case '9':
                    i = 0;
                    floatingPoint = false;
                    while (true) { 
                        if (!Character.isDigit((char)ch) && ch != '-' && ch != '+' && ch != '.' && ch != 'E') { 
                            unget(ch);
                            try { 
                                if (floatingPoint) { 
                                    fconst = Double.valueOf(new String(sconst, 0, i)).doubleValue();
                                    return XML_FCONST;
                                } else { 
                                    iconst = Long.parseLong(new String(sconst, 0, i));
                                    return XML_ICONST;
                                }
                            } catch (NumberFormatException x) { 
                                throw new XMLImportException(line, column, "Bad XML file format");
                            }
                        }
                        if (i == size) { 
                            throw new XMLImportException(line, column, "Bad XML file format");
                        }
                        sconst[i++] = (char)ch;
                        if (ch == '.') { 
                            floatingPoint = true;
                        }
                        ch = get();
                    }
                  default:
                    i = 0;
                    while (isLetterOrDigit((char)ch) || ch == '-' || ch == ':' || ch == '_' || ch == '.') {
                        if (i == size) { 
                            throw new XMLImportException(line, column, "Bad XML file format");
                        }
                        if (ch == '-') { 
                            ch = '$';
                        }                                
                        sconst[i++] = (char)ch;
                        ch = get();
                    }
                    unget(ch);
                    if (i == 0) { 
                        throw new XMLImportException(line, column, "Bad XML file format");
                    }
                    ident = new String(sconst, 0, i);
                    return XML_IDENT;
                }
            }
        }
        
        final String getIdentifier() { 
            return ident;
        }

        final String getString() { 
            return new String(sconst, 0, slen);
        }

        final long getInt() { 
            return iconst;
        }

        final double getReal() { 
            return fconst;
        }

        final int    getLine() { 
            return line;
        }

        final int    getColumn() { 
            return column;
        }
    }
}
