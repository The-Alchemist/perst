package org.garret.perst.reflect;

/**
 * Replacement of java.lang.reflect.Method class
 */
public class Member { 
    Type owner;
    int modifiers;
    
    protected Member(int modifiers) { 
        this.modifiers = modifiers;
    }

    /**
     * Get member modifiers. There are not predefined modifiers, such as PUBLIC - 
     * iterpretation of modifiers bits is application dependent
     * @return bitmask of member modifiers
     */
    public int getModifiers() { 
        return modifiers;
    }

    /**
     * Get declaring clsss descriptor 
     * @return descriptor of the class containing this member
     */
    public Type getDeclaringClass() {
        return owner;
    }
}    
    