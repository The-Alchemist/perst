package org.garret.perst.impl;

import java.io.*;
import java.util.*;
import org.garret.perst.*;

/**
 * Class for exporting the whole database or pert of the database in XML format.
 * Exported data can be used later by XMLImporter to perform recover or conversion of
 * database or be used by some other data consumer.
 * This class should be used in this way:
 *     Writer writer = new BufferedWriter(new FileWriter("dump.xml"));
 *     XMLExporter exporter = new XMLExporter(db, writer);
 *     exporter.exportDatabase();
 *     writer.close();
 */
public class XMLExporter 
{ 
    /**
     * Constructor of XML exporter
     * @param storage source storage
     * @param writer XML writer
     */
    public XMLExporter(Storage storage, Writer writer) { 
        this.storage = (StorageImpl)storage;
        this.writer = writer;
    }

    /**
     * Export all objects from the database
     */
    public void exportDatabase() 
    {
        exportCluster(storage.getRoot());
    }

    /**
     * Export all objects referenced from the specified root object
     * @param root root of the object cluster
     */
    public void exportCluster(IPersistent root) 
    { 
        try { 
            if (storage.encoding != null) { 
                writer.write("<?xml version=\"1.0\" encoding=\"" + storage.encoding + "\"?>\n");
            } else { 
                writer.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
            }
            if (root == null || root.getOid() == 0) { 
                writer.write("<database root=\"0\"/>");
                return;
            }
            int rootOid = root.getOid();
            writer.write("<database root=\"" + rootOid + "\">\n");
            exportedBitmap = new int[(storage.currIndexSize + 31) / 32];
            markedBitmap = new int[(storage.currIndexSize + 31) / 32];
            markedBitmap[rootOid >> 5] |= 1 << (rootOid & 31);
            int nExportedObjects;
            XMLOutputStream out = new XMLOutputStream();
            do { 
                nExportedObjects = 0;
                for (int i = 0; i < markedBitmap.length; i++) { 
                    int mask = markedBitmap[i];
                    if (mask != 0) { 
                        for (int j = 0, bit = 1; j < 32; j++, bit <<= 1) { 
                            if ((mask & bit) != 0) { 
                                int oid = (i << 5) + j;
                                exportedBitmap[i] |= bit;
                                markedBitmap[i] &= ~bit;
                                try { 
                                    IPersistent obj = storage.getObjectByOID(oid);
                                    if (obj instanceof BtreeCompoundIndex) { 
                                        exportCompoundIndex(oid, (BtreeCompoundIndex)obj);
                                    } else if (obj instanceof Btree) { 
                                        exportIndex(oid, (Btree)obj);
                                    } else { 
                                        String className = exportIdentifier(obj.getClass().getName());
                                        writer.write(" <" + className + " id=\"" + oid + "\">\n");
                                        out.reset();
                                        obj.writeObject(out);
                                        writer.write(" </" + className + ">\n");
                                    }
                                    nExportedObjects += 1;
                                } catch (StorageError x) { 
                                    if (storage.listener != null) {
                                        storage.listener.objectNotExported(oid, x);
                                    } else { 
                                        System.err.println("XML export failed for object " + oid + ": " + x);
                                    }
                                }
                            }
                        }
                    }
                }                            
            } while (nExportedObjects != 0);
            writer.write("</database>\n");   
            writer.flush(); // writer should be closed by calling code
        } catch (IOException x) { 
            throw new StorageError(StorageError.IO_EXCEPTION, x);
        }
    }

    final String exportIdentifier(String name) { 
        return name.replace('$', '-');
    }

    final void exportIndex(int oid, Btree index) throws IOException 
    { 
        String name = index.getClass().getName();
        writer.write(" <" + name + " id=\"" + index.getOid() + "\" unique=\"" + (index.unique ? '1' : '0') 
                     + "\" type=\"" + index.type + "\">\n");
        exportTree(index);
        writer.write(" </" + name + ">\n");
    }

    final void exportCompoundIndex(int oid, BtreeCompoundIndex index) throws IOException
    { 
        String name = index.getClass().getName();
        writer.write(" <" + name + " id=\"" + index.getOid() + "\" unique=\"" + (index.unique ? '1' : '0') + "\""); 
        compoundKeyTypes = index.types;
        for (int i = 0; i < compoundKeyTypes.length; i++) { 
            writer.write(" type" + i + "=\"" + compoundKeyTypes[i] + "\"");
        }
        writer.write(">\n");
        exportTree(index);
        compoundKeyTypes = null;
        writer.write(" </" + name + ">\n");
    }

    final int exportKey(byte[] body, int offs, int size, int type) throws IOException
    {
        switch (type) { 
        case Types.Boolean:
            writer.write(body[offs++] != 0 ? '1' : '0');
            break;
        case Types.Byte:
            writer.write(Integer.toString(body[offs++]));
            break;
        case Types.Char:
            writer.write(Integer.toString((char)Bytes.unpack2(body, offs)));
            offs += 2;
            break;
        case Types.Short:
            writer.write(Integer.toString(Bytes.unpack2(body, offs)));
            offs += 2;
            break;
        case Types.Int:
        case Types.Object:
            writer.write(Integer.toString(Bytes.unpack4(body, offs)));
            offs += 4;
            break;
        case Types.Long:
            writer.write(Long.toString(Bytes.unpack8(body, offs)));
            offs += 8;
            break;
        case Types.Float:
            writer.write(Float.toString(Float.intBitsToFloat(Bytes.unpack4(body, offs))));
            offs += 4;
            break;
        case Types.Double:
            writer.write(Double.toString(Double.longBitsToDouble(Bytes.unpack8(body, offs))));
            offs += 8;
            break;
        case Types.String:
            for (int i = 0; i < size; i++) { 
                exportChar((char)Bytes.unpack2(body, offs));
                offs += 2;
            }
            break;
        case Types.ArrayOfByte:
            for (int i = 0; i < size; i++) { 
                byte b = body[offs++];
                writer.write(hexDigit[(b >>> 4) & 0xF]);
                writer.write(hexDigit[b & 0xF]);
            }
            break;
        case Types.Date:
            {
                long msec = Bytes.unpack8(body, offs);
                offs += 8;
                if (msec >= 0) { 
                    writer.write(new Date(msec).toString());
                } else { 
                    writer.write("null");
                }
                break;
            }
        default:
            Assert.that(false);
        }
        return offs;
    }

    final void exportCompoundKey(byte[] body, int offs, int size, int type) throws IOException 
    { 
        Assert.that(type == Types.ArrayOfByte);
        int end = offs + size;
        for (int i = 0; i < compoundKeyTypes.length; i++) { 
            type = compoundKeyTypes[i];
            if (type == Types.ArrayOfByte) { 
                size = Bytes.unpack4(body, offs);
                offs += 4;
            } else if (type == Types.String) { 
                size = Bytes.unpack2(body, offs);
                offs += 2;
            }
            writer.write(" key" + i + "=\"");
            offs = exportKey(body, offs, size, type); 
            writer.write("\"");
        }
        Assert.that(offs == end);
    }

    final void exportAssoc(int oid, byte[] body, int offs, int size, int type) throws IOException
    {
        writer.write("  <ref id=\"" + oid + "\"");
        if ((exportedBitmap[oid >> 5] & (1 << (oid & 31))) == 0) { 
            markedBitmap[oid >> 5] |= 1 << (oid & 31);
        }
        if (compoundKeyTypes != null) { 
            exportCompoundKey(body, offs, size, type);
        } else { 
            writer.write(" key=\"");
            exportKey(body, offs, size, type);
            writer.write("\"");
        }
        writer.write("/>\n");
    }

    final void exportChar(char ch) throws IOException { 
        switch (ch) {
        case '<':
            writer.write("&lt;");
            break;
        case '>':
            writer.write("&gt;");
            break;
        case '&':
            writer.write("&amp;");
            break;
        case '"':
            writer.write("&quot;");
            break;
        case '\'':
            writer.write("&apos;");
            break;
        default:
            writer.write(ch);
        }
    }

    final void exportDate(Date date) throws IOException { 
        writer.write(date == null ? "null" : "\"" + date + "\"");
    }

    final void exportString(String s) throws IOException { 
        if (s == null) { 
            writer.write("null");
        } else { 
            writer.write("\"");                    
            for (int i = 0, n = s.length(); i < n; i++) { 
                exportChar(s.charAt(i));
            }
            writer.write("\"");   
        }
    }

    static final char hexDigit[] = {
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'
    };

    final void exportBinary(byte[] value) throws IOException { 
        if (value == null) { 
            writer.write("null");
        } else {
            writer.write('\"');
            for (int i = 0; i < value.length; i++) {
                byte b = value[i];
                writer.write(hexDigit[(b >>> 4) & 0xF]);
                writer.write(hexDigit[b & 0xF]);
            }
            writer.write('\"');
        }
    }
    
    final void exportRef(IPersistent obj) throws IOException { 
        if (obj == null) { 
            writer.write("null");
        } else { 
            int oid = obj.getOid(); 
            if (obj instanceof PersistentStub) { 
                writer.write("<ref id=\"" + oid + "\"/>");
            } else { 
                writer.write("<ref id=\"" + oid + "\" type=\"" +  exportIdentifier(obj.getClass().getName()) + "\"/>");
            }
            if (oid != 0 && (exportedBitmap[oid >> 5] & (1 << (oid & 31))) == 0) { 
                markedBitmap[oid >> 5] |= 1 << (oid & 31);
            }
        }
    }

    class XMLOutputStream implements IOutputStream { 
        int columnNo;
        int nestingLevel;

        public void reset() { 
            columnNo = 0;
            nestingLevel = 0;
        }
                
        public OutputStream getOutputStream() { 
            return null;
        }

        private final void beginColumn(int type) throws IOException { 
            if (nestingLevel != 0) { 
                writer.write("  <object type=\"" + type + "\">");
            } else { 
                writer.write("  <column no=\"" + (++columnNo) + "\" type=\"" + type + "\">");
            }
        }
        
        private final void endColumn() throws IOException  {
            writer.write(nestingLevel != 0 ? "</object>\n" : "</column>\n");
        }

        public void writeByte(byte v) { 
            try { 
                beginColumn(Types.Byte);
                writer.write(Integer.toString(v));
                endColumn();
            } catch (IOException x) { 
                throw new StorageError(StorageError.IO_EXCEPTION, x);
            }
        }

        public void writeBoolean(boolean v) { 
            try { 
                beginColumn(Types.Boolean);
                writer.write(v ? '1' : '0');
                endColumn();
            } catch (IOException x) { 
                throw new StorageError(StorageError.IO_EXCEPTION, x);
            }
        }

        public void writeChar(char v) { 
            try { 
                beginColumn(Types.Char);
                writer.write(Integer.toString(v));
                endColumn();
            } catch (IOException x) { 
                throw new StorageError(StorageError.IO_EXCEPTION, x);
            }
        }

        public void writeShort(short v) { 
            try { 
                beginColumn(Types.Short);
                writer.write(Integer.toString(v));
                endColumn();
            } catch (IOException x) { 
                throw new StorageError(StorageError.IO_EXCEPTION, x);
            }
        }

        public void writeInt(int v) { 
            try { 
                beginColumn(Types.Int);
                writer.write(Integer.toString(v));
                endColumn();
            } catch (IOException x) { 
                throw new StorageError(StorageError.IO_EXCEPTION, x);
            }
        }

        public void writeLong(long v) { 
            try { 
                beginColumn(Types.Long);
                writer.write(Long.toString(v));
                endColumn();
            } catch (IOException x) { 
                throw new StorageError(StorageError.IO_EXCEPTION, x);
            }
        }

        public void writeFloat(float v) { 
            try { 
                beginColumn(Types.Float);
                writer.write(Float.toString(v));
                endColumn();
            } catch (IOException x) { 
                throw new StorageError(StorageError.IO_EXCEPTION, x);
            }
        }

        public void writeDouble(double v) { 
            try { 
                beginColumn(Types.Double);
                writer.write(Double.toString(v));
                endColumn();
            } catch (IOException x) { 
                throw new StorageError(StorageError.IO_EXCEPTION, x);
            }
        }

        public void writeString(String s) { 
            try { 
                beginColumn(Types.String);
                exportString(s);
                endColumn();
            } catch (IOException x) { 
                throw new StorageError(StorageError.IO_EXCEPTION, x);
            }
        }

        public void writeDate(Date v) { 
            try { 
                beginColumn(Types.Date);
                exportDate(v);
                endColumn();
            } catch (IOException x) { 
                throw new StorageError(StorageError.IO_EXCEPTION, x);
            }
        }
        
        public void writeObject(IPersistent obj) { 
            try { 
                beginColumn(Types.Object);
                exportRef(obj);
                endColumn();
            } catch (IOException x) { 
                throw new StorageError(StorageError.IO_EXCEPTION, x);
            }
        }

        public void writeLink(Link v) { 
            try { 
                beginColumn(Types.Link);
                if (v == null) { 
                    writer.write("null");
                } else { 
                    writer.write('\n');
                    for (int i = 0, n = v.size(); i < n; i++) { 
                        writer.write("   <element index=\"" + i + "\">");
                        exportRef(v.getRaw(i));
                        writer.write("</element>\n");
                    }
                    writer.write("  ");
                }
                endColumn();
            } catch (IOException x) { 
                throw new StorageError(StorageError.IO_EXCEPTION, x);
            }
        }

        public void writeVector(Vector v) { 
            try { 
                beginColumn(Types.Vector);
                if (v == null) { 
                    writer.write("null");
                } else { 
                    writer.write('\n');
                    nestingLevel += 1;
                    for (int i = 0, n = v.size(); i < n; i++) { 
                        writer.write("   <element index=\"" + i + "\">");
                        write(v.elementAt(i));
                        writer.write("   </element>\n");
                    }
                    writer.write("  ");
                    nestingLevel -= 1;
                }
                endColumn();
            } catch (IOException x) { 
                throw new StorageError(StorageError.IO_EXCEPTION, x);
            }
        }

        public void write(Object v) {
            if (v == null) { 
                try { 
                    beginColumn(Types.Null);
                    writer.write("null");
                    endColumn();
                } catch (IOException x) { 
                    throw new StorageError(StorageError.IO_EXCEPTION, x);
                }
            } else if (v instanceof IPersistent) { 
                writeObject((IPersistent)v);
            } else if (v instanceof String) { 
                writeString((String)v);
            } else if (v instanceof Integer) {
                writeInt(((Integer)v).intValue());
            } else if (v instanceof Long) { 
                writeLong(((Long)v).longValue());
            } else if (v instanceof Byte) { 
                writeByte(((Byte)v).byteValue());
            } else if (v instanceof Short) { 
                writeShort(((Short)v).shortValue());
            } else if (v instanceof Character) { 
                writeChar(((Character)v).charValue());
            } else if (v instanceof Boolean) { 
                writeBoolean(((Boolean)v).booleanValue());
            } else if (v instanceof Float) { 
                writeFloat(((Float)v).floatValue());
            } else if (v instanceof Double) { 
                writeDouble(((Double)v).doubleValue());
            } else if (v instanceof Date) { 
                writeDate((Date)v);
            } else if (v instanceof Link) {
                writeLink((Link)v);
            } else if (v instanceof boolean[]) { 
                writeArrayOfBoolean((boolean[])v);
            } else if (v instanceof byte[]) { 
                writeArrayOfByte((byte[])v);
            } else if (v instanceof char[]) { 
                writeArrayOfChar((char[])v);
            } else if (v instanceof short[]) { 
                writeArrayOfShort((short[])v);
            } else if (v instanceof int[]) { 
                writeArrayOfInt((int[])v);
            } else if (v instanceof long[]) { 
                writeArrayOfLong((long[])v);
            } else if (v instanceof float[]) { 
                writeArrayOfFloat((float[])v);
            } else if (v instanceof double[]) { 
                writeArrayOfDouble((double[])v);
            } else if (v instanceof String[]) { 
                writeArrayOfString((String[])v);
            } else if (v instanceof Date[]) { 
                writeArrayOfDate((Date[])v);
            } else if (v instanceof IPersistent[]) { 
                writeArrayOfObject((IPersistent[])v);
            } else {
                throw new StorageError(StorageError.UNSUPPORTED_TYPE);
            }        
        }
        
        public void writeHashtable(Hashtable v) { 
            try { 
                beginColumn(Types.Hashtable);
                if (v == null) { 
                    writer.write("null");
                } else { 
                    writer.write('\n');
                    nestingLevel += 1;
                    Enumeration e = v.keys(); 
                    while (e.hasMoreElements())  {
                        Object key = e.nextElement();
                        Object value = v.get(key);
                        writer.write("   <entry>\n");
                        writer.write("    <key>\n");
                        write(key);
                        writer.write("    </key>\n");
                        writer.write("    <value>\n");
                        write(value);
                        writer.write("    </value>\n");
                        writer.write("   </entry>\n");
                    }
                    writer.write("  ");
                    nestingLevel -= 1;
                }
                endColumn();
            } catch (IOException x) { 
                throw new StorageError(StorageError.IO_EXCEPTION, x);
            }
        }

        public void writeArrayOfBoolean(boolean[] v) { 
            try { 
                beginColumn(Types.ArrayOfBoolean);
                if (v == null) { 
                    writer.write("null");
                } else { 
                    writer.write('\n');
                    for (int i = 0, n = v.length; i < n; i++) { 
                        writer.write("   <element index=\"" + i + "\">");
                        writer.write(v[i] ? '1' : '0');
                        writer.write("   </element>\n");
                    }
                    writer.write("  ");
                }
                endColumn();
            } catch (IOException x) { 
                throw new StorageError(StorageError.IO_EXCEPTION, x);
            }
        }        
        
        
        public void writeArrayOfByte(byte[] v) { 
            try { 
                beginColumn(Types.ArrayOfByte);
                exportBinary(v);
                endColumn();
            } catch (IOException x) { 
                throw new StorageError(StorageError.IO_EXCEPTION, x);
            }
        }        
        
        public void writeArrayOfShort(short[] v) { 
            try { 
                beginColumn(Types.ArrayOfShort);
                if (v == null) { 
                    writer.write("null");
                } else { 
                    writer.write('\n');
                    for (int i = 0, n = v.length; i < n; i++) { 
                        writer.write("   <element index=\"" + i + "\">" + Integer.toString(v[i]) + "</element>\n");
                    }
                    writer.write("  ");
                }
                endColumn();
            } catch (IOException x) { 
                throw new StorageError(StorageError.IO_EXCEPTION, x);
            }
        }        
        
        public void writeArrayOfChar(char[] v) { 
            try { 
                beginColumn(Types.ArrayOfChar);
                if (v == null) { 
                    writer.write("null");
                } else { 
                    writer.write('\n');
                    for (int i = 0, n = v.length; i < n; i++) { 
                        writer.write("   <element index=\"" + i + "\">" + Integer.toString(v[i]) + "</element>\n");
                    }
                    writer.write("  ");
                }
                endColumn();
            } catch (IOException x) { 
                throw new StorageError(StorageError.IO_EXCEPTION, x);
            }
        }        
        
        
        public void writeArrayOfInt(int[] v) { 
            try { 
                beginColumn(Types.ArrayOfInt);
                if (v == null) { 
                    writer.write("null");
                } else { 
                    writer.write('\n');
                    for (int i = 0, n = v.length; i < n; i++) { 
                        writer.write("   <element index=\"" + i + "\">" + Integer.toString(v[i]) + "</element>\n");
                    }
                    writer.write("  ");
                }
                endColumn();
            } catch (IOException x) { 
                throw new StorageError(StorageError.IO_EXCEPTION, x);
            }
        }        
    
        
        public void writeArrayOfLong(long[] v) { 
            try { 
                beginColumn(Types.ArrayOfLong);
                if (v == null) { 
                    writer.write("null");
                } else { 
                    writer.write('\n');
                    for (int i = 0, n = v.length; i < n; i++) { 
                        writer.write("   <element index=\"" + i + "\">" + Long.toString(v[i]) + "</element>\n");
                    }
                    writer.write("  ");
                }
                endColumn();
            } catch (IOException x) { 
                throw new StorageError(StorageError.IO_EXCEPTION, x);
            }
        }        
        
        public void writeArrayOfFloat(float[] v) { 
            try { 
                beginColumn(Types.ArrayOfFloat);
                if (v == null) { 
                    writer.write("null");
                } else { 
                    writer.write('\n');
                    for (int i = 0, n = v.length; i < n; i++) { 
                        writer.write("   <element index=\"" + i + "\">" + Float.toString(v[i]) + "</element>\n");
                    }
                    writer.write("  ");
                }
                endColumn();
            } catch (IOException x) { 
                throw new StorageError(StorageError.IO_EXCEPTION, x);
            }
        }        
    
        
        public void writeArrayOfDouble(double[] v) { 
            try { 
                beginColumn(Types.ArrayOfDouble);
                if (v == null) { 
                    writer.write("null");
                } else { 
                    writer.write('\n');
                    for (int i = 0, n = v.length; i < n; i++) { 
                        writer.write("   <element index=\"" + i + "\">" + Double.toString(v[i]) + "</element>\n");
                    }
                    writer.write("  ");
                }
                endColumn();
            } catch (IOException x) { 
                throw new StorageError(StorageError.IO_EXCEPTION, x);
            }
        }        
        
        public void writeArrayOfString(String[] v) { 
            try { 
                beginColumn(Types.ArrayOfString);
                if (v == null) { 
                    writer.write("null");
                } else { 
                    writer.write('\n');
                    for (int i = 0, n = v.length; i < n; i++) { 
                        writer.write("   <element index=\"" + i + "\">");
                        exportString(v[i]);
                        writer.write("</element>\n");
                    }
                    writer.write("  ");
                }
                endColumn();
            } catch (IOException x) { 
                throw new StorageError(StorageError.IO_EXCEPTION, x);
            }
        }        
    
        public void writeArrayOfDate(Date[] v) { 
            try { 
                beginColumn(Types.ArrayOfDate);
                if (v == null) { 
                    writer.write("null");
                } else { 
                    writer.write('\n');
                    for (int i = 0, n = v.length; i < n; i++) { 
                        writer.write("   <element index=\"" + i + "\">");
                        exportDate(v[i]);
                        writer.write("</element>\n");
                    }
                    writer.write("  ");
                }
                endColumn();
            } catch (IOException x) { 
                throw new StorageError(StorageError.IO_EXCEPTION, x);
            }
        }        

        public void writeArrayOfObject(IPersistent[] v) { 
            try { 
                beginColumn(Types.ArrayOfObject);
                if (v == null) { 
                    writer.write("null");
                } else { 
                    writer.write('\n');
                    for (int i = 0, n = v.length; i < n; i++) { 
                        writer.write("   <element index=\"" + i + "\">");
                        exportRef(v[i]);
                        writer.write("</element>\n");
                    }
                    writer.write("  ");
                }
                endColumn();
            } catch (IOException x) { 
                throw new StorageError(StorageError.IO_EXCEPTION, x);
            }
        }
    }

    private final void exportTree(Btree tree) throws IOException
    { 
        if (tree.root != 0) { 
            exportPage(tree.root, tree.type, tree.height);
        }
    }        

    private final void exportPage(int pageId, int type, int height) throws IOException
    {
        Page pg = storage.getPage(pageId);
        try { 
            int i, n = BtreePage.getnItems(pg);
            if (--height != 0) {
                if (type == Types.String || type == Types.ArrayOfByte) { // page of strings
                    for (i = 0; i <= n; i++) { 
                        exportPage(BtreePage.getKeyStrOid(pg, i), type, height);
                    }
                } else { 
                    for (i = 0; i <= n; i++) { 
                        exportPage(BtreePage.getReference(pg, BtreePage.maxItems-i-1), type, height);
                    }
                }
            } else { 
                if (type == Types.String || type == Types.ArrayOfByte) { // page of strings
                    for (i = 0; i < n; i++) {
                        exportAssoc(BtreePage.getKeyStrOid(pg, i), 
                                    pg.data, 
                                    BtreePage.firstKeyOffs + BtreePage.getKeyStrOffs(pg, i),
                                    BtreePage.getKeyStrSize(pg, i),
                                    type);
                    }
                } else { 
                    for (i = 0; i < n; i++) { 
                        exportAssoc(BtreePage.getReference(pg, BtreePage.maxItems-1-i), 
                                    pg.data, 
                                    BtreePage.firstKeyOffs + i*ClassDescriptor.sizeof[type], 
                                    ClassDescriptor.sizeof[type],
                                    type);
                                             
                    }
                }
            }
        } finally { 
            storage.pool.unfix(pg);
        }
    }



    private StorageImpl storage;
    private Writer      writer;
    private int[]       markedBitmap;
    private int[]       exportedBitmap;
    private int[]       compoundKeyTypes;
}
