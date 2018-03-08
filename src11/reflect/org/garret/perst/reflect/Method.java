package org.garret.perst.reflect;

/**
 * Replacement of java.lang.reflect.Field.
 * This class should be created by programmer. To make it possible to invoke method,
 * programmer should derive its own subclass and override invoke method:
 * new Method(int.class, "someMethod", new Class[]{int.class}) { public Object invoke(Object obj, Object[] paramers) { return ((MyClass)obj).someMethod(((Integer)parameters[0]).intValue()); } }
 */
public class Method extends Member 
{ 
    String  name;
    Class   returnType;
    Class[] parameterTypes;

    /** 
     * Constructor of method descriptor
     * @param returnType method return type. As far as J2ME provides no classes for builtin types, use correspndent wrapper classes, 
     * i.e. instead of int.class, specify Integer.class. 
     * In case of void method specify null as return type
     * @param name method name
     * @param parameterTypes method parameter types. As far as J2ME provides no classes for builtin types, use correspndent wrapper classes, 
     * i.e. instead of int.class, specify Integer.class. 
     */
    public Method(Class returnType, String name, Class[] parameterTypes) { 
        this(returnType, name, parameterTypes, 0);
    }

    /** 
     * Constructor of method descriptor
     * @param returnType method return type. As far as J2ME provides no classes for builtin types, use correspndent wrapper classes, 
     * i.e. instead of int.class, specify Integer.class. 
     * In case of void method specify null as return type
     * @param name method name
     * @param parameterTypes method parameter types. As far as J2ME provides no classes for builtin types, use correspndent wrapper classes, 
     * i.e. instead of int.class, specify Integer.class. 
     * @param modifiers bitmask of method modifiers. There are not predefined modifiers, such as PUBLIC - 
     * iterpretation of modifiers bits is application dependent
     */
    public Method(Class returnType, String name, Class[] parameterTypes, int modifiers) { 
        super(modifiers);
        this.returnType = returnType;
        this.name = name;
        this.parameterTypes = parameterTypes == null ? new Class[0] : parameterTypes;
    }    

    /** 
     * Get name of the method
     * @return method name
     */
    public String getName() { 
        return name;
    }

    /** 
     * Get method returtn type
     * @return method return type
     */
    public Class getReturnType() {
        return returnType;
    }    

    /** 
     * Get types of method parameters
     * @return array with method parameter types
     */
    public Class[] getParameterTypes() {
        return parameterTypes;
    }    

    /**
     * Invoke method. This method is expected to be overriden by programmer in derived class. 
     * @param obj target objects
     * @param args method arguments
     * @return value returned by invoked method
     */
    public Object invoke(Object obj, Object[] args) { 
        throw new RuntimeException("Abstract method invocation");
    }

    /**
     * Invoke method without parameter
     * @param obj target objects
     * @return value returned by invoked method
     */
    public Object invoke(Object obj) { 
        return invoke(obj, NO_PARAMS);
    }

    static private final Object[] NO_PARAMS = new Object[0];
}        


