package org.garret.perst.reflect;

import java.util.Hashtable;

/**
 * Class descriptor provided by programmer.
 * This class provides reflection methods from java.lang.Class which
 * are not present in J2ME.
 */                         
public class Type 
{ 
    Class[] interfaces;
    Class   baseClass;
    Class   thisClass;
    Field[] fields;
    Method[] methods;
    Hashtable fieldMap;
    Hashtable methodMap;

    /**
     * Create new class descriptor
     * @param thisClass described class
     * @param baseClass base class or null if there is no other base class except java.lang.Object
     * @param interfaces implemented inteerface (should be non-null - if class implements no interfaces, 
     * then empty array should be passed
     * @param fields list of fields declared by this class (not including fields of base class). Null is equivalent to empty array.
     * @param methods list of methods declared by this class (not including methods of base class). Null is equivalent to empty array.
     */
    public Type(Class thisClass, 
                Class baseClass,
                Class[] interfaces,
                Field[] fields,
                Method[] methods)
    {
        this.thisClass = thisClass;
        this.baseClass = baseClass;
        this.fields = fields == null ? new Field[0] : fields;
        this.methods = methods == null ? new Method[0] : methods;
        this.interfaces = interfaces;
        fieldMap = new Hashtable();
        for (int i = 0; i < fields.length; i++) { 
            fields[i].owner = this;
            fieldMap.put(fields[i].name, fields[i]);
        }
        methodMap = new Hashtable();
        for (int i = 0; i < methods.length; i++) { 
            methods[i].owner = this;
            methodMap.put(methods[i].name, methods[i]);
        }
        ReflectionProvider.getInstance().register(this);
    }            

    /**
     * Get class fully qualified name
     * @return class fully qualified name
     */
    public String getName() { 
        return thisClass.getName();
    }

    /**
     * Get class described by this class descriptor 
     * @return described class
     */
    public Class getDescribedClass() { 
        return thisClass;
    }

    /**
     * Get superclass for this class
     * @return suprtclass or null if superclass is java.lang.Object
     */
    public Class getSuperclass() { 
        return baseClass;
    }

    /**
     * Get interfaces implemented by this class
     * @return array of interfaces implemented by this class
     */
    public Class[] getInterfaces() { 
        return interfaces;
    }

    /**
     * Get fields declared by this class
     * @return array of fields declared by this class
     */
    public Field[] getDeclaredFields() {
        return fields;
    }
    
    /**
     * Get methods declared by this class
     * @return array of methods declared by this class
     */
    public Method[] getDeclaredMethods() {
        return methods;
    }

    /** Get field by name
     * @param name field name
     * @return field with this name or null if no such field in the class
     */
    public Field getDeclaredField(String name) { 
        return (Field)fieldMap.get(name);
    }

    /** 
     * Get field by name in this class and all subclasses
     * @param name field name
     * @return field with this name or null if no such field in the class or its subclasses
     */
    public Field getField(String name) { 
        Type scope = this;
        do { 
            Field fld = scope.getDeclaredField(name);                
            if (fld != null) { 
                return fld;
            }
            if (scope.baseClass != null) { 
                scope = ReflectionProvider.getInstance().getType(scope.baseClass);
            } else {
                break;
            }
        } while (scope != null);
        return null;
    }

    /** 
     * Get method declared in this class by name (method overloading is not supported)
     * @param name method name
     * @return method with specified name or null if no such method in the class
     */
    public Method getDeclaredMethod(String name) { 
        return (Method)methodMap.get(name);
    }

    /** 
     * Get method by name in this class and all subclasses (method overloading is not supported)
     * @param name method name
     * @return method with this name or null if no such method in the class and its subclasses
     */
    public Method getMethod(String name) { 
        Type scope = this;
        do { 
            Method mth = scope.getDeclaredMethod(name);                
            if (mth != null) { 
                return mth;
            }
            if (scope.baseClass != null) { 
                scope = ReflectionProvider.getInstance().getType(scope.baseClass);
            } else { 
                break;
            }
        } while (scope != null);
        return null;
    }

    /**
     * Create new object instance of this class. Redefine this method in the derived class
     * if you want to provide more efficient implementation of class instantiation or
     * want to allow creation of instances of non-public classes without public default constructors
     * @return created instance 
     */
    public Object newInstance() throws InstantiationException, IllegalAccessException{ 
        return thisClass.newInstance();
    }
}
