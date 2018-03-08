package org.garret.perst.impl;

import org.garret.perst.reflect.*;
import org.garret.perst.*;
import java.util.Date;

/**
 * Implementation of multidimensional reflection comparator using reflection
 */
public class ReflectionMultidimensionalComparator extends MultidimensionalComparator 
{ 
    private String   className;
    private String[] fieldNames;
    private boolean  treateZeroAsUndefinedValue;
    private int[]    fieldTypes;

    transient private Type cls;
    transient private Field[] fields;
    transient private ClassDescriptor desc;

    public void writeObject(IOutputStream out) {
        out.writeString(className);
        out.writeArrayOfString(fieldNames);
        out.writeBoolean(treateZeroAsUndefinedValue);
        out.writeArrayOfInt(fieldTypes);
    }

    public void readObject(IInputStream in) {
        className = in.readString();
        fieldNames = in.readArrayOfString();
        treateZeroAsUndefinedValue = in.readBoolean();
        fieldTypes = in.readArrayOfInt();
        cls = ReflectionProvider.getInstance().getType(className);
        locateFields();
    }

    private final void locateFields() 
    {
        if (fieldNames == null) { 
            fields = cls.getDeclaredFields();
        } else { 
            fields = new Field[fieldNames.length];
            for (int i = 0; i < fields.length; i++) { 
                fields[i] = cls.getField(fieldNames[i]);
                if (fields[i] == null) { 
                    throw new StorageError(StorageError.INDEXED_FIELD_NOT_FOUND, className + "." + fieldNames[i]);
                }
            }
        }
    }

    public ReflectionMultidimensionalComparator(Storage storage, Class cls, String[] fieldNames, boolean treateZeroAsUndefinedValue) 
    { 
        super(storage);
        this.cls = ReflectionProvider.getInstance().getType(cls);
        this.fieldNames = fieldNames;
        this.treateZeroAsUndefinedValue = treateZeroAsUndefinedValue;
        className = ClassDescriptor.getClassName(cls); 
        locateFields();
        fieldTypes = new int[fieldNames.length];
        for (int i = 0; i < fieldTypes.length; i++) { 
            fieldTypes[i] = Types.getTypeCode(fields[i].getType());
        }
    }

    public ReflectionMultidimensionalComparator() {}
    
    public int compare(Object m1, Object m2, int i)
    {
        switch (fieldTypes[i]) {
        case Types.Byte:
        {
            byte v1 = fields[i].getByte(m1);
            byte v2 = fields[i].getByte(m2);
            return treateZeroAsUndefinedValue 
                ? (v1 == 0 ? LEFT_UNDEFINED : v2 == 0 ? RIGHT_UNDEFINED : v1 < v2 ? LT : v1 == v2 ? EQ : GT) 
                : (v1 < v2 ? LT : v1 == v2 ? EQ : GT);
        }
        case Types.Char:
        {
            char v1 = fields[i].getChar(m1);
            char v2 = fields[i].getChar(m2);
            return treateZeroAsUndefinedValue 
                ? (v1 == 0 ? LEFT_UNDEFINED : v2 == 0 ? RIGHT_UNDEFINED : v1 < v2 ? LT : v1 == v2 ? EQ : GT)
                : (v1 < v2 ? LT : v1 == v2 ? EQ : GT);
        }
        case Types.Short:
        {
            short v1 = fields[i].getShort(m1);
            short v2 = fields[i].getShort(m2);
            return treateZeroAsUndefinedValue 
                ? (v1 == 0 ? LEFT_UNDEFINED : v2 == 0 ? RIGHT_UNDEFINED : v1 < v2 ? LT : v1 == v2 ? EQ : GT) 
                : (v1 < v2 ? LT : v1 == v2 ? EQ : GT);
        }
        case Types.Int:
        {
            int v1 = fields[i].getInt(m1);
            int v2 = fields[i].getInt(m2);
            return treateZeroAsUndefinedValue 
                ? (v1 == 0 ? LEFT_UNDEFINED : v2 == 0 ? RIGHT_UNDEFINED : v1 < v2 ? LT : v1 == v2 ? EQ : GT)
                : (v1 < v2 ? LT : v1 == v2 ? EQ : GT);
        }
        case Types.Long:
        {
            long v1 = fields[i].getLong(m1);
            long v2 = fields[i].getLong(m2);
            return treateZeroAsUndefinedValue 
                ? (v1 == 0 ? LEFT_UNDEFINED : v2 == 0 ? RIGHT_UNDEFINED : v1 < v2 ? LT : v1 == v2 ? EQ : GT)
                : (v1 < v2 ? LT : v1 == v2 ? EQ : GT);
        }
        case Types.Float:
        {
            float v1 = fields[i].getFloat(m1);
            float v2 = fields[i].getFloat(m2);
            return treateZeroAsUndefinedValue 
                ? (v1 == 0 ? LEFT_UNDEFINED : v2 == 0 ? RIGHT_UNDEFINED : v1 < v2 ? LT : v1 == v2 ? EQ : GT)
                : (v1 < v2 ? LT : v1 == v2 ? EQ : GT);
        }
        case Types.Double:
        {
            double v1 = fields[i].getDouble(m1);
            double v2 = fields[i].getDouble(m2);
            return treateZeroAsUndefinedValue 
                ? (v1 == 0 ? LEFT_UNDEFINED : v2 == 0 ? RIGHT_UNDEFINED : v1 < v2 ? LT : v1 == v2 ? EQ : GT)
                : (v1 < v2 ? LT : v1 == v2 ? EQ : GT);
        }
        case Types.String:
        {
            String v1 = (String)fields[i].get(m1);
            String v2 = (String)fields[i].get(m2);
            if (v1 == null && v2 == null) { 
                return EQ;
            } else if (v1 == null) { 
                return LEFT_UNDEFINED;
            } else if (v2 == null) { 
                return RIGHT_UNDEFINED;
            } else { 
                int diff = v1.compareTo(v2);
                return diff < 0 ? LT : diff == 0 ? EQ : GT;
            }
        }
        case Types.Date:
        {
            Date v1 = (Date)fields[i].get(m1);
            Date v2 = (Date)fields[i].get(m2);
            if (v1 == null && v2 == null) { 
                return EQ;
            } else if (v1 == null) { 
                return LEFT_UNDEFINED;
            } else if (v2 == null) { 
                return RIGHT_UNDEFINED;
            } else { 
                long t1 = v1.getTime();
                long t2 = v2.getTime();
                return t1 < t2 ? LT : t1 == t2 ? EQ : GT;
            }
        }
        default:
            Assert.that(false);
            return EQ;
        }
    }

    public int getNumberOfDimensions() { 
        return fields.length;
    }

    public IPersistent cloneField(IPersistent obj, int i) { 
        IPersistent clone;
        try { 
            clone = (IPersistent)cls.newInstance();
        } catch (Exception x) { 
            throw new RuntimeException(x.getMessage());
        }
        fields[i].set(clone, fields[i].get(obj));
        return clone;
    }
}

