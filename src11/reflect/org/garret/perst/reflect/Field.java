package org.garret.perst.reflect;


/**
 * Replacement of java.lang.reflect.Field.
 * This class should be created by programmer. To provide access the field, 
 * programmer should derive its own subclass and override field set/get methods:
 * new Field(String.class, "someField") { public Object get(Object obj) { return ((MyClass)obj).someField; } }
 */
public class Field extends Member 
{ 
    String name;
    Class  type;

    /**
     * Constructor of field descriptor 
     * @param type field type. As far as J2ME provides no classes for builtin types, use correspndent wrapper classes, 
     * i.e. instead of int.class, specify Integer.class
     * @param name field name
     */
    public Field(Class type, String name) { 
        this(type, name, 0);
    }

    /**
     * Constructor of field descriptor 
     * @param type field type. As far as J2ME provides no classes for builtin types, use correspndent wrapper classes, 
     * i.e. instead of int.class, specify Integer.class
     * @param name field name
     * @param modifiers bitmask of method modifiers. There are not predefined modifiers, such as PUBLIC - 
     * iterpretation of modifiers bits is application dependent
     */
    public Field(Class type, String name, int modifiers) { 
        super(modifiers);
        this.type = type;
        this.name = name;
    }    

    /** 
     * Get name of the field
     * @return field name
     */
    public String getName() {
	return name;
    }

    /** 
     * Get type of the field
     * @return field type
     */
    public Class getType() {
	return type;
    }

    /** 
     * Get value of the field. This method is expected to be overridden in derived class by programmer.
     * @param obj target objects
     * @return field value
     */
    public Object get(Object obj) { 
        throw new RuntimeException("Getter not defined");
    }

    /** 
     * Get boolean value of the field. This method may be overridden in derived class by programmer for boolean field.
     * If this method is not overridden,then default implementation tries to extract boolean value from object returned by get method.
     * @param obj target objects
     * @return boolean field value
     */
    public boolean getBoolean(Object obj) { 
        return ((Boolean)get(obj)).booleanValue();
    }

    /** 
     * Get byte value of the field. This method may be overridden in derived class by programmer for byte field.
     * If this method is not overridden,then default implementation tries to extract byte value from object returned by get method.
     * @param obj target objects
     * @return byte field value
     */
    public byte getByte(Object obj) { 
        return ((Byte)get(obj)).byteValue();
    }

    /** 
     * Get char value of the field. This method may be overridden in derived class by programmer for char field.
     * If this method is not overridden,then default implementation tries to extract char value from object returned by get method.
     * @param obj target objects
     * @return char field value
     */
    public char getChar(Object obj) { 
        return ((Character)get(obj)).charValue();
    }

    /** 
     * Get short value of the field. This method may be overridden in derived class by programmer for short field.
     * If this method is not overridden,then default implementation tries to extract short value from object returned by get method.
     * @param obj target objects
     * @return short field value
     */
    public short getShort(Object obj) { 
        return ((Short)get(obj)).shortValue();
    }


    /** 
     * Get int value of the field. This method may be overridden in derived class by programmer for int field.
     * If this method is not overridden,then default implementation tries to extract int value from object returned by get method.
     * @param obj target objects
     * @return int field value
     */
    public int getInt(Object obj) { 
        if (obj instanceof Integer) { 
            return ((Integer)get(obj)).intValue();
        } else if (obj instanceof Short) { 
            return ((Short)get(obj)).shortValue();
        } else if (obj instanceof Character) { 
            return ((Character)get(obj)).charValue();
        } else { 
            return ((Byte)get(obj)).byteValue();
        }
    }

    /** 
     * Get long value of the field. This method may be overridden in derived class by programmer for long field.
     * If this method is not overridden,then default implementation tries to extract long value from object returned by get method.
     * @param obj target objects
     * @return long field value
     */
    public long getLong(Object obj) { 
        if (obj instanceof Long) { 
             return ((Long)get(obj)).longValue();
        } else if (obj instanceof Short) { 
            return ((Short)get(obj)).shortValue();
        } else if (obj instanceof Byte) { 
            return ((Byte)get(obj)).byteValue();
        } else if (obj instanceof Character) { 
            return ((Character)get(obj)).charValue();
        } else { 
            return ((Integer)get(obj)).intValue();
        }
    }

    /** 
     * Get float value of the field. This method may be overridden in derived class by programmer for float field.
     * If this method is not overridden,then default implementation tries to extract float value from object returned by get method.
     * @param obj target objects
     * @return float field value
     */
    public float getFloat(Object obj) { 
        return ((Float)get(obj)).floatValue();
    }

    /** 
     * Get double value of the field. This method may be overridden in derived class by programmer for double field.
     * If this method is not overridden,then default implementation tries to extract double value from object returned by get method.
     * @param obj target objects
     * @return double field value
     */
    public double getDouble(Object obj) { 
        return obj instanceof Double ? ((Double)get(obj)).doubleValue() : ((Float)get(obj)).floatValue();
    }

    /** 
     * Set value of the field. This method is expected to be overridden in derived class by programmer.
     * @param obj target objects
     * @param value field value
     */
    public void set(Object obj, Object value) { 
        throw new RuntimeException("Setter not defined");
    }

    /** 
     * Set boolean value of the field. This method may be overridden in derived class by programmer for boolean field.
     * If this method is not overridden, then default implementation tries to create wrapper object and assign it to the field using set method.
     * @param obj target objects
     * @param value field value
     */
    public void setBoolean(Object obj, boolean value) { 
        set(obj, new Boolean(value));
    }

    /** 
     * Set byte value of the field. This method may be overridden in derived class by programmer for byte field.
     * If this method is not overridden, then default implementation tries to create wrapper object and assign it to the field using set method.
     * @param obj target objects
     * @param value field value
     */
    public void setByte(Object obj, byte value) { 
        set(obj, new Byte(value));
    }

    /** 
     * Set char value of the field. This method may be overridden in derived class by programmer for char field.
     * If this method is not overridden, then default implementation tries to create wrapper object and assign it to the field using set method.
     * @param obj target objects
     * @param value field value
     */
    public void setChar(Object obj, char value) { 
        set(obj, new Character(value));
    }

    /** 
     * Set short value of the field. This method may be overridden in derived class by programmer for short field.
     * If this method is not overridden, then default implementation tries to create wrapper object and assign it to the field using set method.
     * @param obj target objects
     * @param value field value
     */
    public void setShort(Object obj, short value) { 
        set(obj, new Short(value));
    }

    /** 
     * Set int value of the field. This method may be overridden in derived class by programmer for int field.
     * If this method is not overridden, then default implementation tries to create wrapper object and assign it to the field using set method.
     * @param obj target objects
     * @param value field value
     */
    public void setInt(Object obj, int value) { 
        set(obj, new Integer(value));
    }

    /** 
     * Set long value of the field. This method may be overridden in derived class by programmer for long field.
     * If this method is not overridden, then default implementation tries to create wrapper object and assign it to the field using set method.
     * @param obj target objects
     * @param value field value
     */
    public void setLong(Object obj, long value) { 
        set(obj, new Long(value));
    }

    /** 
     * Set float value of the field. This method may be overridden in derived class by programmer for float field.
     * If this method is not overridden, then default implementation tries to create wrapper object and assign it to the field using set method.
     * @param obj target objects
     * @param value field value
     */
    public void setFloat(Object obj, float value) { 
        set(obj, new Float(value));
    }

    /** 
     * Set double value of the field. This method may be overridden in derived class by programmer for double field.
     * If this method is not overridden, then default implementation tries to create wrapper object and assign it to the field using set method.
     * @param obj target objects
     * @param value field value
     */
    public void setDouble(Object obj, double value) { 
        set(obj, new Double(value));
    }
}
