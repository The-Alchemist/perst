package org.garret.perst.impl;

public class Number 
{ 
    public static double doubleValue(Object value) { 
        if (value instanceof Double) { 
            return ((Double)value).doubleValue();
        } else if (value instanceof Float) { 
            return (double)((Float)value).floatValue();
        } else if (value instanceof Integer) { 
            return (double)((Integer)value).intValue();
        } else if (value instanceof Short) { 
            return (double)((Short)value).shortValue();
        } else if (value instanceof Byte) { 
            return (double)((Byte)value).byteValue();
        } else if (value instanceof Character) { 
            return (double)((Character)value).charValue();
        } else if (value instanceof Long) { 
            return (double)((Long)value).longValue();
        } else { 
            throw new NumberFormatException();
        }
    }    

    public static float floatValue(Object value) { 
        if (value instanceof Double) { 
            return (float)((Double)value).doubleValue();
        } else if (value instanceof Float) { 
            return ((Float)value).floatValue();
        } else if (value instanceof Integer) { 
            return (float)((Integer)value).intValue();
        } else if (value instanceof Short) { 
            return (float)((Short)value).shortValue();
        } else if (value instanceof Byte) { 
            return (float)((Byte)value).byteValue();
        } else if (value instanceof Character) { 
            return (float)((Character)value).charValue();
        } else if (value instanceof Long) { 
            return (float)((Long)value).longValue();
        } else { 
            throw new NumberFormatException();
        }
    }    

    public static long longValue(Object value) { 
        if (value instanceof Double) { 
            return (long)((Double)value).doubleValue();
        } else if (value instanceof Float) { 
            return (long)((Float)value).floatValue();
        } else if (value instanceof Integer) { 
            return (long)((Integer)value).intValue();
        } else if (value instanceof Short) { 
            return (long)((Short)value).shortValue();
        } else if (value instanceof Byte) { 
            return (long)((Byte)value).byteValue();
        } else if (value instanceof Character) { 
            return (long)((Character)value).charValue();
        } else if (value instanceof Long) { 
            return ((Long)value).longValue();
        } else { 
            throw new NumberFormatException();
        }
    }

    public static int intValue(Object value) { 
        if (value instanceof Double) { 
            return (int)((Double)value).doubleValue();
        } else if (value instanceof Float) { 
            return (int)((Float)value).floatValue();
        } else if (value instanceof Integer) { 
            return ((Integer)value).intValue();
        } else if (value instanceof Short) { 
            return (int)((Short)value).shortValue();
        } else if (value instanceof Byte) { 
            return (int)((Byte)value).byteValue();
        } else if (value instanceof Character) { 
            return (int)((Character)value).charValue();
        } else if (value instanceof Long) { 
            return (int)((Long)value).longValue();
        } else { 
            throw new NumberFormatException();
        }
    }

    public static short shortValue(Object value) { 
        if (value instanceof Double) { 
            return (short)((Double)value).doubleValue();
        } else if (value instanceof Float) { 
            return (short)((Float)value).floatValue();
        } else if (value instanceof Integer) { 
            return (short)((Integer)value).intValue();
        } else if (value instanceof Short) { 
            return ((Short)value).shortValue();
        } else if (value instanceof Byte) { 
            return (short)((Byte)value).byteValue();
        } else if (value instanceof Character) { 
            return (short)((Character)value).charValue();
        } else if (value instanceof Long) { 
            return (short)((Long)value).longValue();
        } else { 
            throw new NumberFormatException();
        }
    }

    public static byte byteValue(Object value) { 
        if (value instanceof Double) { 
            return (byte)((Double)value).doubleValue();
        } else if (value instanceof Float) { 
            return (byte)((Float)value).floatValue();
        } else if (value instanceof Integer) { 
            return (byte)((Integer)value).intValue();
        } else if (value instanceof Short) { 
            return (byte)((Short)value).shortValue();
        } else if (value instanceof Byte) { 
            return ((Byte)value).byteValue();
        } else if (value instanceof Character) { 
            return (byte)((Character)value).charValue();
        } else if (value instanceof Long) { 
            return (byte)((Long)value).longValue();
        } else { 
            throw new NumberFormatException();
        }
    }

    public static boolean isNumber(Object value) { 
        return value instanceof Double 
            || value instanceof Float
            || value instanceof Integer
            || value instanceof Long
            || value instanceof Short
            || value instanceof Byte
            || value instanceof Character;
    } 

    public static boolean isRealNumber(Object value) { 
       return value instanceof Double || value instanceof Float;
    }
}