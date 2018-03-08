package org.garret.perst;

/**
 * Enumeration of Java types supported by Perst.Lite
 * This enumeration is used to describe type code in Perst container constructors.
 */
public class Types 
{
    public static final int Boolean          = 0;
    public static final int Byte             = 1;
    public static final int Char             = 2;
    public static final int Short            = 3;
    public static final int Int              = 4;
    public static final int Long             = 5;
    public static final int Float            = 6;
    public static final int Double           = 7;
    public static final int String           = 8;
    public static final int Date             = 9;
    public static final int Object           = 10;
    public static final int Link             = 11;
    public static final int ArrayOfBoolean   = 20;
    public static final int ArrayOfByte      = 21;
    public static final int ArrayOfChar      = 22;
    public static final int ArrayOfShort     = 23;
    public static final int ArrayOfInt       = 24;
    public static final int ArrayOfLong      = 25;
    public static final int ArrayOfFloat     = 26;
    public static final int ArrayOfDouble    = 27;
    public static final int ArrayOfString    = 28;
    public static final int ArrayOfDate      = 29;
    public static final int ArrayOfObject    = 30;

    public static int getTypeCode(Class c) { 
        String name = c.getName();
        if (c == java.lang.Boolean.class) { 
            return Boolean;
        } else if (c == java.lang.Byte.class) { 
            return Byte;
        } else if (c == java.lang.Character.class) { 
            return Char;
        } else if (c == java.lang.Short.class) { 
            return Short;
        } else if (c == java.lang.Integer.class) { 
            return Int;
        } else if (c == java.lang.Long.class) { 
            return Long;
        } else if (c == java.lang.Float.class) { 
            return Float;
        } else if (c == java.lang.Double.class) { 
            return Double;
        } else if (c == java.lang.String.class) { 
            return String;
        } else if (c == java.util.Date.class) { 
            return Date;
        } else if (c == java.util.Vector.class) { 
            return Vector;
        } else if (c == java.util.Hashtable.class) { 
            return Hashtable;
        } else if (c == boolean[].class) { 
            return ArrayOfBoolean;
        } else if (c == byte[].class) { 
            return ArrayOfByte;
        } else if (c == char[].class) { 
            return ArrayOfChar;
        } else if (c == short[].class) { 
            return ArrayOfShort;
        } else if (c == int[].class) { 
            return ArrayOfInt;
        } else if (c == long[].class) { 
            return ArrayOfLong;
        } else if (c == float[].class) { 
            return ArrayOfFloat;
        } else if (c == double[].class) { 
            return ArrayOfDouble;
        } else if (c == java.lang.String.class) { 
            return ArrayOfString;
        } else if (c == java.util.Date[].class) { 
            return ArrayOfDate;
        } else if (c == java.lang.Object[].class) { 
            return ArrayOfObject;
        } else if (org.garret.perst.Link.class.isAssignableFrom(c)) { 
            return Link;
        } else { 
            return Object;
        }
    }

    /*
     * For internal use only
     */
    public static final int Null = 31;
    public static final int AsciiString = 32;
    public static final int ShortAsciiString = 33;
    public static final int Vector           = 34;
    public static final int Hashtable        = 35;
    public static final int ShortArrayOfByte = 36;

    public static String getSignature(int type) { 
        return signature[type];
    }

    private static final String signature[] = {
        "boolean", 
        "byte",
        "char",
        "short",
        "int",
        "long",
        "float",
        "double",
        "String",
        "Date",
        "Object",
        "Link",
        "",
        "",
        "", 
        "", 
        "", 
        "", 
        "", 
        "", 
        "ArrayOfBoolean",
        "ArrayOfByte",
        "ArrayOfChar",
        "ArrayOfShort",
        "ArrayOfInt",
        "ArrayOfLong",
        "ArrayOfFloat",
        "ArrayOfDouble",
        "ArrayOfString",
        "ArrayOfDate",
        "ArrayOfObject",
        "Null",
        "AsciiString",
        "ShortAsciiString",
        "Vector",
        "Hashtable",
        "ShortArrayOfByte"
    };
}