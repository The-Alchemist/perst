package org.garret.perst.impl;
import  org.garret.perst.*;

import org.garret.perst.reflect.*;
import java.util.Date;

class BtreeMultiFieldIndex extends Btree implements FieldIndex { 
    String   className;
    String[] fieldName;
    int[]    types;

    transient Type    cls;
    transient Field[] fld;

    public void writeObject(IOutputStream out) {
        super.writeObject(out);
        out.writeString(className);
        out.writeArrayOfString(fieldName);
        out.writeArrayOfInt(types);
    }

    public void readObject(IInputStream in) {
        super.readObject(in);
        className = in.readString();
        fieldName = in.readArrayOfString();
        types = in.readArrayOfInt();
        cls = ReflectionProvider.getInstance().getType(className);
        locateFields();
    }

    public BtreeMultiFieldIndex() {}
    
    BtreeMultiFieldIndex(Class cls, String[] fieldName, boolean unique) {
        this.cls = ReflectionProvider.getInstance().getType(cls);
        this.unique = unique;
        this.fieldName = fieldName;        
        this.className = ClassDescriptor.getClassName(cls);
        locateFields();
        type = Types.ArrayOfByte;        
        types = new int[fieldName.length];
        for (int i = 0; i < types.length; i++) {
            types[i] = checkType(Types.getTypeCode(fld[i].getType()));
        }
    }

    private final void locateFields() 
    {
        fld = new Field[fieldName.length];
        for (int i = 0; i < fieldName.length; i++) {
            fld[i] = cls.getField(fieldName[i]);
            if (fld[i] == null) { 
                throw new StorageError(StorageError.INDEXED_FIELD_NOT_FOUND, className + "." + fieldName[i]);
            }
        }
    }

    public Class getIndexedClass() { 
        return cls.getDescribedClass();
    }

    public String[] getKeyFields() { 
        return fieldName;
    }

    int compareByteArrays(byte[] key, byte[] item, int offs, int lengtn) { 
        int o1 = 0;
        int o2 = offs;
        byte[] a1 = key;
        byte[] a2 = item;
        for (int i = 0; i < fld.length && o1 < key.length; i++) {
            int diff = 0;
            switch (types[i]) { 
              case Types.Boolean:
              case Types.Byte:
                diff = a1[o1++] - a2[o2++];
                break;
              case Types.Short:
                diff = Bytes.unpack2(a1, o1) - Bytes.unpack2(a2, o2);
                o1 += 2;
                o2 += 2;
                break;
              case Types.Char:
                diff = (char)Bytes.unpack2(a1, o1) - (char)Bytes.unpack2(a2, o2);
                o1 += 2;
                o2 += 2;
                break;
              case Types.Int:
              case Types.Object:
              {
                  int i1 = Bytes.unpack4(a1, o1);
                  int i2 = Bytes.unpack4(a2, o2);
                  diff = i1 < i2 ? -1 : i1 == i2 ? 0 : 1;
                  o1 += 4;
                  o2 += 4;
                  break;
              }
              case Types.Long:
              case Types.Date:
              {
                  long l1 = Bytes.unpack8(a1, o1);
                  long l2 = Bytes.unpack8(a2, o2);
                  diff = l1 < l2 ? -1 : l1 == l2 ? 0 : 1;
                  o1 += 8;
                  o2 += 8;
                  break;
              }
              case Types.Float:
              {
                  float f1 = Float.intBitsToFloat(Bytes.unpack4(a1, o1));
                  float f2 = Float.intBitsToFloat(Bytes.unpack4(a2, o2));
                  diff = f1 < f2 ? -1 : f1 == f2 ? 0 : 1;
                  o1 += 4;
                  o2 += 4;
                  break;
              }
              case Types.Double:
              {
                  double d1 = Double.longBitsToDouble(Bytes.unpack8(a1, o1));
                  double d2 = Double.longBitsToDouble(Bytes.unpack8(a2, o2));
                  diff = d1 < d2 ? -1 : d1 == d2 ? 0 : 1;
                  o1 += 8;
                  o2 += 8;
                  break;
              }
              case Types.String:
              {
                  int len1 = Bytes.unpack4(a1, o1);
                  int len2 = Bytes.unpack4(a2, o2);
                  o1 += 4;
                  o2 += 4;
                  int len = len1 < len2 ? len1 : len2;
                  while (--len >= 0) { 
                      diff = (char)Bytes.unpack2(a1, o1) - (char)Bytes.unpack2(a2, o2);
                      if (diff != 0) { 
                          return diff;
                      }
                      o1 += 2;
                      o2 += 2;
                  }
                  diff = len1 - len2;
                  break;
              }
              case Types.ArrayOfByte:
              {
                  int len1 = Bytes.unpack4(a1, o1);
                  int len2 = Bytes.unpack4(a2, o2);
                  o1 += 4;
                  o2 += 4;
                  int len = len1 < len2 ? len1 : len2;
                  while (--len >= 0) { 
                      diff = a1[o1++] - a2[o2++];
                      if (diff != 0) { 
                          return diff;
                      }
                  }
                  diff = len1 - len2;
                  break;
              }
              default:
                Assert.failed("Invalid type");
            }
            if (diff != 0) { 
                return diff;
            }
        }
        return 0;
    }

    String convertString(Object s) { 
        return (String)s;
    }

    Object unpackByteArrayKey(Page pg, int pos) {
        int offs = BtreePage.firstKeyOffs + BtreePage.getKeyStrOffs(pg, pos);
        byte[] data = pg.data;
        Object values[] = new Object[fld.length];

        for (int i = 0; i < fld.length; i++) {
            Object v = null;
            switch (types[i]) { 
              case Types.Boolean:
                v = new Boolean(data[offs++] != 0);
                break;
              case Types.Byte:
                v = new Byte(data[offs++]);
                break;
              case Types.Short:
                v = new Short(Bytes.unpack2(data, offs));
                offs += 2;
                break;
              case Types.Char:
                v = new Character((char)Bytes.unpack2(data, offs));
                offs += 2;
                break;
              case Types.Int:
                v = new Integer(Bytes.unpack4(data, offs));
                offs += 4;
                break;
              case Types.Object:
              {
                  int oid = Bytes.unpack4(data, offs);
                  v = oid == 0 ? null : ((StorageImpl)getStorage()).lookupObject(oid);
                  offs += 4;
                  break;
              }
              case Types.Long:
                v = new Long(Bytes.unpack8(data, offs));
                offs += 8;
                break;
              case Types.Date:
              {
                  long msec = Bytes.unpack8(data, offs);
                  v = msec == -1 ? null : new Date(msec);
                  offs += 8;
                  break;
              }
              case Types.Float:
                v = new Float(Float.intBitsToFloat(Bytes.unpack4(data, offs)));
                offs += 4;
                break;
              case Types.Double:
                v = new Double(Double.longBitsToDouble(Bytes.unpack8(data, offs)));
                offs += 8;
                break;
              case Types.String:
              {
                  int len = Bytes.unpack4(data, offs);
                  offs += 4;
                  char[] sval = new char[len];
                  for (int j = 0; j < len; j++) { 
                      sval[j] = (char)Bytes.unpack2(data, offs);
                      offs += 2;
                  }
                  v = new String(sval);
                  break;
              }
              case Types.ArrayOfByte:
              {
                  int len = Bytes.unpack4(data, offs);
                  offs += 4;
                  byte[] bval = new byte[len];
                  System.arraycopy(data, offs, bval, 0, len);
                  offs += len;
                  break;
              }
              default:
                Assert.failed("Invalid type");
            }
            values[i] = v;
        }
        return values;
    }


    private Key extractKey(IPersistent obj) { 
        try { 
            ByteBuffer buf = new ByteBuffer();
            int dst = 0;
            for (int i = 0; i < fld.length; i++) { 
                Field f = (Field)fld[i];
                switch (types[i]) {
                  case Types.Boolean:
                    buf.extend(dst+1);
                    buf.arr[dst++] = (byte)(f.getBoolean(obj) ? 1 : 0);
                    break;
                  case Types.Byte:
                    buf.extend(dst+1);
                    buf.arr[dst++] = f.getByte(obj);
                    break;
                  case Types.Short:
                    buf.extend(dst+2);
                    Bytes.pack2(buf.arr, dst, f.getShort(obj));
                    dst += 2;
                    break;
                  case Types.Char:
                    buf.extend(dst+2);
                    Bytes.pack2(buf.arr, dst, (short)f.getChar(obj));
                    dst += 2;
                    break;
                  case Types.Int:
                    buf.extend(dst+4);
                    Bytes.pack4(buf.arr, dst, f.getInt(obj));
                    dst += 4;
                    break;
                  case Types.Object:
                  {
                      IPersistent p = (IPersistent)f.get(obj);
                      buf.extend(dst+4);
                      if (p != null) { 
                          if (!p.isPersistent())
                          {
                              getStorage().makePersistent(p);
                          }                        
                          Bytes.pack4(buf.arr, dst, p.getOid());
                      } else { 
                          Bytes.pack4(buf.arr, dst, 0);
                      }
                      dst += 4;
                      break;
                  }
                  case Types.Long:
                    buf.extend(dst+8);
                    Bytes.pack8(buf.arr, dst, f.getLong(obj));
                    dst += 8;
                    break;
                  case Types.Date:
                  {
                      Date d = (Date)f.get(obj);
                      buf.extend(dst+8);
                      Bytes.pack8(buf.arr, dst, d == null ? -1 : d.getTime());
                      dst += 8;
                      break;
                  }
                  case Types.Float:
                    buf.extend(dst+4);
                    Bytes.pack4(buf.arr, dst, Float.floatToIntBits(f.getFloat(obj)));
                    dst += 4;
                    break;
                  case Types.Double:
                    buf.extend(dst+8);
                    Bytes.pack8(buf.arr, dst, Double.doubleToLongBits(f.getDouble(obj)));
                    dst += 8;
                    break;
                  case Types.String:
                  {
                      buf.extend(dst+4);
                      String str = convertString(f.get(obj));
                      if (str != null) { 
                          int len = str.length();
                          Bytes.pack4(buf.arr, dst, len);
                          dst += 4;
                          buf.extend(dst + len*2);
                          for (int j = 0; j < len; j++) { 
                              Bytes.pack2(buf.arr, dst, (short)str.charAt(j));
                              dst += 2;
                          }
                      } else { 
                          Bytes.pack4(buf.arr, dst, 0);
                          dst += 4;
                      }
                      break;
                  }
                  case Types.ArrayOfByte:
                  {
                      buf.extend(dst+4);
                      byte[] arr = (byte[])f.get(obj);
                      if (arr != null) { 
                          int len = arr.length;
                          Bytes.pack4(buf.arr, dst, len);
                          dst += 4;                          
                          buf.extend(dst + len);
                          System.arraycopy(arr, 0, buf.arr, dst, len);
                          dst += len;
                      } else { 
                          Bytes.pack4(buf.arr, dst, 0);
                          dst += 4;
                      }
                      break;
                  }
                  default:
                    Assert.failed("Invalid type");
                }
            }
            return new Key(buf.toArray());
        } catch (Exception x) { 
            throw new StorageError(StorageError.ACCESS_VIOLATION, x);
        }
    }
            
    private Key convertKey(Key key) { 
        if (key == null) { 
            return null;
        }
        if (key.type != Types.ArrayOfObject) { 
            throw new StorageError(StorageError.INCOMPATIBLE_KEY_TYPE);
        }
        Object[] values = (Object[])key.oval;
        ByteBuffer buf = new ByteBuffer();
        int dst = 0;
        for (int i = 0; i < values.length; i++) { 
            Object v = values[i];
            switch (types[i]) {
              case Types.Boolean:
                buf.extend(dst+1);
                buf.arr[dst++] = (byte)(((Boolean)v).booleanValue() ? 1 : 0);
                break;
              case Types.Byte:
                buf.extend(dst+1);
                buf.arr[dst++] = Number.byteValue(v);
                break;
              case Types.Short:
                buf.extend(dst+2);
                Bytes.pack2(buf.arr, dst, Number.shortValue(v));
                dst += 2;
                break;
              case Types.Char:
                buf.extend(dst+2);
                Bytes.pack2(buf.arr, dst, Number.shortValue(v));
                dst += 2;
                break;
              case Types.Int:
                buf.extend(dst+4);
                Bytes.pack4(buf.arr, dst, Number.intValue(v));
                dst += 4;
                break;
              case Types.Object:
                buf.extend(dst+4);
                Bytes.pack4(buf.arr, dst, v == null ? 0 : ((IPersistent)v).getOid());
                dst += 4;
                break;
              case Types.Long:
                buf.extend(dst+8);
                Bytes.pack8(buf.arr, dst, Number.longValue(v));
                dst += 8;
                break;
              case Types.Date:
                buf.extend(dst+8);
                Bytes.pack8(buf.arr, dst, v == null ? -1 : ((Date)v).getTime());
                dst += 8;
                break;
              case Types.Float:
                buf.extend(dst+4);
                Bytes.pack4(buf.arr, dst, Float.floatToIntBits(Number.floatValue(v)));
                dst += 4;
                break;
              case Types.Double:
                buf.extend(dst+8);
                Bytes.pack8(buf.arr, dst, Double.doubleToLongBits(Number.doubleValue(v)));
                dst += 8;
                break;
              case Types.String:
              {
                  buf.extend(dst+4);
                  if (v != null) { 
                      String str = convertString(v);
                      int len = str.length();
                      Bytes.pack4(buf.arr, dst, len);
                      dst += 4;
                      buf.extend(dst + len*2);
                      for (int j = 0; j < len; j++) { 
                          Bytes.pack2(buf.arr, dst, (short)str.charAt(j));
                          dst += 2;
                      }
                  } else { 
                      Bytes.pack4(buf.arr, dst, 0);
                      dst += 4;
                  }
                  break;
              }
              case Types.ArrayOfByte:
              {
                  buf.extend(dst+4);
                  if (v != null) { 
                      byte[] arr = (byte[])v;
                      int len = arr.length;
                      Bytes.pack4(buf.arr, dst, len);
                      dst += 4;                          
                      buf.extend(dst + len);
                      System.arraycopy(arr, 0, buf.arr, dst, len);
                      dst += len;
                  } else { 
                      Bytes.pack4(buf.arr, dst, 0);
                      dst += 4;
                  }
                  break;
              }
              default:
                Assert.failed("Invalid type");
            }
        }
        return new Key(buf.toArray(), key.inclusion != 0);
    }
            

    public boolean put(IPersistent obj) {
        return super.put(extractKey(obj), obj);
    }

    public IPersistent set(IPersistent obj) {
        return super.set(extractKey(obj), obj);
    }

    public void remove(IPersistent obj) {
        super.remove(extractKey(obj), obj);
    }

    public IPersistent remove(Key key) {
        return super.remove(convertKey(key));
    }
    
    public boolean containsObject(IPersistent obj) {
        Key key = extractKey(obj);
        if (unique) { 
            return super.get(key) != null;
        } else { 
            IPersistent[] mbrs = get(key, key);
            for (int i = 0; i < mbrs.length; i++) { 
                if (mbrs[i] == obj) { 
                    return true;
                }
            }
            return false;
        }
    }

    public boolean contains(IPersistent obj) {
        Key key = extractKey(obj);
        if (unique) { 
            return super.get(key) != null;
        } else { 
            IPersistent[] mbrs = get(key, key);
            for (int i = 0; i < mbrs.length; i++) { 
                if (mbrs[i].equals(obj)) { 
                    return true;
                }
            }
            return false;
        }
    }

    public void append(IPersistent obj) {
        throw new StorageError(StorageError.UNSUPPORTED_INDEX_TYPE);
    }

    public IPersistent[] get(Key from, Key till) {
        ArrayList list = new ArrayList();
        if (root != 0) { 
            BtreePage.find((StorageImpl)getStorage(), root, convertKey(from), convertKey(till), this, height, list);
        }
        return (IPersistent[])list.toArray(new IPersistent[list.size()]);
    }

    public IPersistent[] toPersistentArray() {
        IPersistent[] arr = new IPersistent[nElems];
        if (root != 0) { 
            BtreePage.traverseForward((StorageImpl)getStorage(), root, type, height, arr, 0);
        }
        return arr;
    }

    public IPersistent get(Key key) {
        return super.get(convertKey(key));
    }

    public Iterator iterator(Key from, Key till, int order) {
        return super.iterator(convertKey(from), convertKey(till), order);
    }

    public Iterator entryIterator(Key from, Key till, int order) {
        return super.entryIterator(convertKey(from), convertKey(till), order);
    }

    public boolean isCaseInsensitive() { 
        return false;
    }
}

class BtreeCaseInsensitiveMultiFieldIndex extends BtreeMultiFieldIndex {    
    BtreeCaseInsensitiveMultiFieldIndex() {}

    BtreeCaseInsensitiveMultiFieldIndex(Class cls, String[] fieldNames, boolean unique) {
        super(cls, fieldNames, unique);
    }

    String convertString(Object s) { 
        return ((String)s).toLowerCase();
    }

    public boolean isCaseInsensitive() { 
        return true;
    }
}
