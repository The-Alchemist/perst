package org.garret.perst.jassist;

import javassist.*;
import javassist.expr.*;

/**
 * This class is designed to be used with 
 * <A href="http://www.csg.is.titech.ac.jp/~chiba/javassist/">JAssist</A> to provide transparent 
 * persistence  by preprocessing protect class files. This translator automatically
 * made persistent capable all class matching specifying name patterns. With this 
 * translator it is not required for application classes to be derived from Persistent class
 * and provide empty default constructor. 
 * Example of usage:
 * <pre>
 * package com.mycompany.mypackage;
 * import org.garret.perst.jassist.PerstTranslator;
 * import javassist.*;
 * public class MyApp { 
 *     public static void main(String[] args) { 
 *         Translatator trans = new PerstTranslator(new String[]{"com.mycompany.*"});
 *         ClassPool pool = ClassPool.getDefault(trans);
 *         Loader cl = new Loader(pool);
 *         cl.run("Main", args);
 *     }
 * }
 * </pre>
 * In this example all classes from <code>com.mycompany.mypackage</code> except 
 * MyApp will be loaded by JAssist class loader and automatically made persistent capable.
 */
public class PerstTranslator implements Translator { 
    protected boolean isPersistent(String className) { 
        for (int i = 0; i < classNamePatterns.length; i++) { 
            String pattern = classNamePatterns[i];
            if (className.equals(pattern) 
                || (pattern.endsWith("*") 
                    && className.startsWith(pattern.substring(0, pattern.length()-1))
                    && !className.endsWith("LoadFactory")
                    && !className.startsWith("org.garret.perst")))
            {
                return true;
            }
        }
        return false;
    }

    /**
     * Create Perst translator which made all classes persistent capable 
     * (excluding <code>java.*</code> and <code>org.garret.perst.*</code> 
     * packages)
     */
    public PerstTranslator() { 
        this(new String[]{"*"});
    }

    /**
     * Create Perst translator with specified list of class name patterns.
     * Classes which fully qualified name matchs one of the patterns are made persistent capable.
     */
    public PerstTranslator(String[] classNamePatterns) { 
        this.classNamePatterns = classNamePatterns;
    }

    public void start(ClassPool pool) throws NotFoundException { 
        persistent = pool.get("org.garret.perst.Persistent");
        persistentInterface = pool.get("org.garret.perst.IPersistent");
        factory = pool.get("org.garret.perst.impl.LoadFactory");
        object = pool.get("java.lang.Object");
        isRecursive = persistent.getDeclaredMethod("recursiveLoading"); 
        constructorParams = new CtClass[]{pool.get("org.garret.perst.impl.ClassDescriptor")};
        serializable = pool.get("org.garret.perst.SelfSerializable");
        pack = serializable.getDeclaredMethod("pack");
        unpack = serializable.getDeclaredMethod("unpack");
        create = factory.getDeclaredMethod("create");
    }

    static String getTypeName(CtClass type) {
        if (type == CtClass.booleanType) { 
            return "Boolean";
        } else if (type == CtClass.charType) { 
            return "Char";
        } else if (type == CtClass.byteType) { 
            return "Byte";
        } else if (type == CtClass.shortType) { 
            return "Short";
        } else if (type == CtClass.intType) { 
            return "Int";
        } else if (type == CtClass.longType) { 
            return "Long";
        } else if (type == CtClass.doubleType) { 
            return "Double";
        } else if (type == CtClass.floatType) { 
            return "Float";
        } else if (type.getName().equals("java.lang.String")) { 
            return "String";
        } else { 
            return "Object";
        }
    }

    private boolean addSerializeMethods(CtClass cc, boolean callSuper)
        throws NotFoundException, CannotCompileException
    {        
        CtField[] fields = cc.getDeclaredFields();
        cc.addInterface(serializable);

        CtMethod m = new CtMethod(pack, cc, null);
        StringBuffer sb = new StringBuffer();
        sb.append(callSuper ? "{super.pack($1);" : "{");
        
        for (int i = 0; i < fields.length; i++) { 
            CtField f = fields[i];
            if ((f.getModifiers() & (Modifier.STATIC|Modifier.TRANSIENT)) == 0) { 
                CtClass type = f.getType();
                String name = f.getName();
                sb.append("$1.write");
                sb.append(getTypeName(type));
                sb.append('(');
                sb.append(name);
                sb.append(");");
            }
        }
        sb.append("}");
        m.setBody(sb.toString());
        cc.addMethod(m);

        m = new CtMethod(unpack, cc, null);
        sb = new StringBuffer();
        sb.append(callSuper ? "{super.unpack($1);" : "{");

        for (int i = 0; i < fields.length; i++) { 
            CtField f = fields[i];
            if ((f.getModifiers() & (Modifier.STATIC|Modifier.TRANSIENT)) == 0) { 
                CtClass type = f.getType();
                String name = f.getName();
                sb.append(name);
                String typeName = getTypeName(type);
                if (typeName.equals("Object")) { 
                    sb.append("=(");
                    sb.append(type.getName());
                    sb.append(")$1.read");
                } else {
                    sb.append("=$1.read");
                }
                sb.append(typeName);
                sb.append("();");
            }
        }
        sb.append("}");
        m.setBody(sb.toString());
        cc.addMethod(m);
        return true;
    }

    private void preprocessMethods(CtClass cc, boolean insertLoad, boolean wrapFieldAccess)  throws CannotCompileException 
    {
        CtMethod[] methods = cc.getDeclaredMethods();
        for (int i = 0; i < methods.length; i++) { 
            CtMethod m = methods[i];            
            if (wrapFieldAccess) { 
                m.instrument(new ExprEditor() { 
                    public void edit(FieldAccess fa) throws CannotCompileException { 
                        try { 
                            if ((fa.getField().getModifiers() & (Modifier.TRANSIENT|Modifier.STATIC)) == 0
                                && fa.getField().getDeclaringClass().subtypeOf(persistentInterface))
                            {
                                if (fa.isWriter()) { 
                                    fa.replace("{ $0.loadAndModify(); $proceed($$); }");
                                }
                                // isSelfReader is my extension of JAssist, if you 
                                // use original version of JAssist comment the 
                                // branch below or replace "else if" with "else".
                                // In first case Perst will not be able to handle
                                // access to foreign (non-this) fields. You should use
                                // getter/setter methods instead. 
                                // In second case access to foreign fields still will be possible,
                                // but with significant degradation of performance and 
                                // increased code size, because in this case before ALL access
                                // to fields of persistent capable object call of load() method
                                // will be inserted.
                                else if (!fa.isSelfReader()) 
                                { 
                                    fa.replace("{ $0.load(); $_ = $proceed($$); }");
                                }
                            }
                        } catch (NotFoundException x) {}
                    }
                });
            }
            if (insertLoad 
                && !"recursiveLoading".equals(m.getName())
                && (m.getModifiers() & (Modifier.STATIC|Modifier.ABSTRACT)) == 0)  
            { 
                m.insertBefore("load();");
            }
        }
    }


    public void onLoad(ClassPool pool, String className)
        throws NotFoundException, CannotCompileException
    {
        onWrite(pool, className);
    }

    public void onWrite(ClassPool pool, String className)
        throws NotFoundException, CannotCompileException 
    {
        CtClass cc = pool.get(className);
        if (cc.isInterface() || cc.isModified()) {
            return;
        }
        try {
            if (isPersistent(className)) {                
                CtClass base = cc.getSuperclass();
                CtConstructor cons = new CtConstructor(constructorParams, cc);            
                if (base == persistent || base == object) { 
                    cons.setBody(null);
                    cc.addConstructor(cons);
                    if (base == object) { 
                        cc.setSuperclass(persistent);                    
                    }
                } else { 
                    String baseName = base.getName();
                    if (!isPersistent(baseName)) { 
                        throw new NotFoundException("Base class " + base.getName()
                                                    + " was not declared as peristent");
                    } 
                    if (!base.isModified()) {
                        onWrite(pool, baseName);
                    }
                    cons.setBody("super($1);");
                    cc.addConstructor(cons);
                }
                preprocessMethods(cc, true, true);
                if (base == persistent || base == object) { 
                    CtMethod m = new CtMethod(isRecursive, cc, null);
                    m.setBody("return false;");
                    cc.addMethod(m);
                    addSerializeMethods(cc, false);
                } else { 
                    addSerializeMethods(cc, true);
                }
                if ((cc.getModifiers() & Modifier.PRIVATE) == 0) { 
                    CtClass f = pool.makeClass(className + "LoadFactory");
                    f.addInterface(factory);
                    CtMethod c = new CtMethod(create, f, null);
                    c.setBody("return new " + className + "($1);");
                    f.addMethod(c);
                    CtNewConstructor.defaultConstructor(f);
                }
            } else { 
                preprocessMethods(cc, 
                                  cc.subtypeOf(persistent) && cc != persistent, 
                                  !className.startsWith("org.garret.perst")); 
            }
        } catch(Exception x) { x.printStackTrace(); }
    }

    CtClass       persistent;
    CtClass       persistentInterface;
    CtClass       object;
    CtClass       factory;
    CtClass[]     constructorParams;
    CtMethod      create;
    CtMethod      isRecursive;
    String[]      classNamePatterns;
    CtClass       serializable;
    CtMethod      pack;
    CtMethod      unpack;
}
            

