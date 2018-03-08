package org.garret.perst.impl;

public class Array { 
    public static int getLength(Object arr) { 
        if (arr instanceof byte[]) { 
            return ((byte[])arr).length;
        } else if (arr instanceof boolean[]) { 
            return ((boolean[])arr).length;
        } else if (arr instanceof char[]) { 
            return ((char[])arr).length;
        } else if (arr instanceof short[]) { 
            return ((short[])arr).length;
        } else if (arr instanceof int[]) { 
            return ((int[])arr).length;
        } else if (arr instanceof long[]) { 
            return ((long[])arr).length;
        } else if (arr instanceof float[]) { 
            return ((float[])arr).length;
        } else if (arr instanceof double[]) { 
            return ((double[])arr).length;
        } else {
            return ((Object[])arr).length;
        } 
    }

    public static Object get(Object arr, int index) { 
        if (arr instanceof byte[]) { 
            return new Byte(((byte[])arr)[index]);
        } else if (arr instanceof boolean[]) { 
            return new Boolean(((boolean[])arr)[index]);
        } else if (arr instanceof char[]) { 
            return new Character(((char[])arr)[index]);
        } else if (arr instanceof short[]) { 
            return new Short(((short[])arr)[index]);
        } else if (arr instanceof int[]) { 
            return new Integer(((int[])arr)[index]);
        } else if (arr instanceof long[]) { 
            return new Long(((long[])arr)[index]);
        } else if (arr instanceof float[]) { 
            return new Float(((float[])arr)[index]);
        } else if (arr instanceof double[]) { 
            return new Double(((double[])arr)[index]);
        } else {
            return ((Object[])arr)[index];
        } 
    }
}
