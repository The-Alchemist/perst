package org.garret.perst.reflect;

import java.util.Hashtable;

/**
 * Replacement of standard Java reflection mechanism for J2ME.
 * Class descriptors (represented by Type class) should be explictely created and registered by programmer in 
 * reflection programmer. Type class provides reflection methods from java.lang.Class which
 * are not present in J2ME
 */                                               
public class ReflectionProvider 
{ 
    Hashtable typeMap = new Hashtable();

    /**
     * Register class descriptor in reflection provides
     * @param type class descriptor
     */
    public void register(Type type) { 
        typeMap.put(type.getName(), type);
    }

    /**
     * Get class descriptor by class
     * @param cls J2ME class
     * @return registered class descriptor for this class or null if there is no such class descriptor
     */
    public Type getType(Class cls) { 
        return getType(cls.getName());
    }

    /**
     * Get class descriptor by class name
     * @param name class name
     * @return registered class descriptor for this class or null if there is no such class descriptor
     */
    public Type getType(String name) { 
        return (Type)typeMap.get(name);
    }

    /**
     * Get instance of reflection provider
     */
    public static ReflectionProvider getInstance() { 
        return instance;
    }

    static ReflectionProvider instance = new ReflectionProvider();
}
 
