import java.util.*;
import java.io.*;

class JavaField { 
    String    name;
    JavaType  type;
    String    cls;
    String    comment;
    JavaField next;
    int       flags;
    
    static final int SERIALIZABLE = 1;
    static final int PROPERTY = 2;
    static final int BINARY = 4;
}

class ClassReferer 
{ 
    static boolean blackberryVerificationBugWorkarround;

    static String getClassRef(String name) { 
        if (blackberryVerificationBugWorkarround) { 
            return name.replace("[]", "_ARRAY") + "_class";
        } else {
            return name + ".class";
        }
    }
    
    static void addClassRef(Writer writer, String name, HashSet set) throws IOException  { 
        if (name != null && set.add(name)) { 
            writer.write(", " + name.replace("[]", "_ARRAY") + "_class=" + name + ".class");
        }
    }
}

class JavaClass { 
    JavaField firstField;
    JavaField lastField;
    JavaMethod firstMethod;
    JavaMethod lastMethod;
    boolean hasReferences;
    String name;
    String baseClass;
    String[] interfaces = new String[0];

    static boolean blackberryVerificationBugWorkarround;

    void addMethod(JavaMethod m) { 
        if (firstMethod == null) { 
            firstMethod = m;
        } else { 
            lastMethod.next = m;
        }
        lastMethod = m;
    }        

    void addField(JavaField f) { 
        if (firstField == null) { 
            firstField = f;
        } else { 
            lastField.next = f;
        }
        lastField = f;
        hasReferences |= f.type.kind == JavaType.tpObject;
    }        

    static void fill(Writer writer, int indent) throws IOException { 
        if (indent < 0) {
            writer.write(' ');
        } else { 
            writer.write(JavaFile.newLine.toCharArray());
            while (--indent >= 0) { 
                writer.write(' ');
            }
        }
    }        

    void isPinnable(Writer writer, int indent) throws IOException { 
        fill(writer, indent);
        if (hasReferences) { 
            writer.write("return false;".toCharArray());
        } else { 
            writer.write("return super.isPinnable();".toCharArray());
        }
    }

    void pack(Writer writer, String param, int indent) throws IOException { 
        fill(writer, indent);
        writer.write(("super.writeObject(" + param + ");").toCharArray());
        for (JavaField f = firstField; f != null; f = f.next) { 
            fill(writer, indent);
            if ((f.flags & JavaField.SERIALIZABLE) != 0) {
                writer.write((f.name + ".writeObject(" + param + ");").toCharArray());
            } else if ((f.flags & JavaField.BINARY) != 0) {
                writer.write((param + ".writeArrayOfByte(" + f.name + ".toByteArray());").toCharArray());
            } else if ((f.flags & JavaField.PROPERTY) != 0) { 
                writer.write(("set" + f.type.cls + "(" + param + ", " + f.name + ");").toCharArray());
            } else { 
                writer.write((param + ".write" + f.type.getTypeName() + "(" + f.name + ");").toCharArray());
            }
        }
    }


    void unpack(Writer writer, String param, int indent) throws IOException { 
        fill(writer, indent);
        writer.write(("super.readObject(" + param + ");").toCharArray());
        for (JavaField f = firstField; f != null; f = f.next) { 
            fill(writer, indent);
            if ((f.flags & JavaField.SERIALIZABLE) != 0) {
                writer.write((f.name + " = new " + f.type.expr + "(); " + f.name + ".readObject(" + param + ");").toCharArray());
            } else if ((f.flags & JavaField.BINARY) != 0) {
                writer.write((f.name + " = new " + f.type.cls + "(" + param + ".readArrayOfByte());").toCharArray());
            } else if ((f.flags & JavaField.PROPERTY) != 0) { 
                writer.write((f.name + " = get" + f.type.cls + "(" + param + ");").toCharArray());
            } else { 
                writer.write((f.name + " = " + (f.type.kind == JavaType.tpObject || f.type.kind == JavaType.tpAny ? "(" + f.cls  + ")" : "") + param + ".read" + f.type.getTypeName() + "();").toCharArray());
            }
        }
    }

    String parseModifiers(String modifiers) { 
        int i, j, n;
        StringBuffer buf = new StringBuffer();
        for (i = 0, n = modifiers.length(); i < n;) { 
            if (Character.isJavaIdentifierStart(modifiers.charAt(i))) { 
                for (j = i+1; j < n && (Character.isJavaIdentifierPart(modifiers.charAt(j)) || modifiers.charAt(j) == '.'); j++);
                if (buf.length() != 0) { 
                    buf.append('|');
                }
                buf.append("Modifier.");
                buf.append(modifiers.substring(i, j));
                i = j;
            } else { 
                i += 1;
            }
        }
        return buf.length() != 0 ? buf.toString() : "0";
    }
                     
    void getClassDescriptor(Writer writer, int indent) throws IOException { 
        fill(writer, indent);
        if (ClassReferer.blackberryVerificationBugWorkarround) { 
            HashSet classRefs = new HashSet();
            writer.write("final Class " + name + "_class=" + name + ".class, " + baseClass + "_class=" + baseClass + ".class");
            classRefs.add(name);
            classRefs.add(baseClass);
            for (int i = 0; i < interfaces.length; i++) { 
                ClassReferer.addClassRef(writer, interfaces[i], classRefs);
            }
            for (JavaField f = firstField; f != null; f = f.next) { 
                ClassReferer.addClassRef(writer, f.type.getClassName(), classRefs);
            }
            for (JavaMethod m = firstMethod; m != null; m = m.next) { 
                ClassReferer.addClassRef(writer, m.returnType.getClassName(), classRefs);
                for (int i = 0; i < m.parameterTypes.length; i++) { 
                    ClassReferer.addClassRef(writer, m.parameterTypes[i].getClassName(), classRefs);
                }
            } 
            writer.write(";");
            fill(writer, indent);
        }

        writer.write("return new Type(" + ClassReferer.getClassRef(name) + ", " + ClassReferer.getClassRef(baseClass) + ", new Class[]{");
        for (int i = 0; i < interfaces.length; i++) { 
            if (i != 0) { 
                writer.write(", ");
            }
            writer.write(ClassReferer.getClassRef(interfaces[i]));
        }
        writer.write("}, new Field[]{" );
        for (JavaField f = firstField; f != null; f = f.next) { 
            fill(writer, indent + SerGen.TAB);
            writer.write("new Field(" + f.type.getTypedef() + ", \"" + f.name + "\"");
            if (f.comment != null) { 
                int modifiers = f.comment.indexOf("@modifier");
                if (modifiers >= 0) {
                    int eol = f.comment.indexOf('\n', modifiers + 10);
                    if (eol < 0) { 
                        eol = f.comment.length();
                    }
                    writer.write(", " + parseModifiers(f.comment.substring(modifiers + 10, eol)));
                }
            }
            writer.write(") {");
            if (f.type.kind <= JavaType.tpDouble) { 
                fill(writer, indent + SerGen.TAB*2);
                writer.write("public Object get(Object obj) { return new " + JavaType.typeWrapper[f.type.kind] + "(((" + name + ")obj)." + f.name + "); }"); 
                fill(writer, indent + SerGen.TAB*2);
                writer.write("public void set(Object obj, Object value) { " + "((" + name + ")obj)." + f.name + " = ((" + JavaType.typeWrapper[f.type.kind] + ")value)." + JavaType.builtinType[f.type.kind] + "Value(); }");
                fill(writer, indent + SerGen.TAB*2);
                writer.write("public " + JavaType.builtinType[f.type.kind] + " get" + f.type.getTypeName() + "(Object obj) { return ((" + name + ")obj)." + f.name + "; }"); 
                fill(writer, indent + SerGen.TAB*2);
                writer.write("public void set" + f.type.getTypeName() + "(Object obj, " + JavaType.builtinType[f.type.kind] 
                             + " value) { " + "((" + name + ")obj)." + f.name + " = value; }");
            } else if ((f.flags & JavaField.BINARY) != 0) { 
                fill(writer, indent + SerGen.TAB*2);
                writer.write("public Object get(Object obj) { return ((" + name + ")obj)." + f.name + ".toByteArray(); }");
                fill(writer, indent + SerGen.TAB*2);
                writer.write("public void set(Object obj, Object value) { " + "((" + name + ")obj)." + f.name 
                             + " = new " + f.type.cls + "((byte[])value); }");
            } else { 
                fill(writer, indent + SerGen.TAB*2);
                writer.write("public Object get(Object obj) { return ((" + name + ")obj)." + f.name + "; }");
                fill(writer, indent + SerGen.TAB*2);
                writer.write("public void set(Object obj, Object value) { " + "((" + name + ")obj)." + f.name 
                             + " = (" + f.type.expr + ")value; }");
            }
            fill(writer, indent + SerGen.TAB);
            writer.write("},");
        }
        writer.write("}, new Method[]{");
        for (JavaMethod m = firstMethod; m != null; m = m.next) { 
            fill(writer, indent + SerGen.TAB);
            writer.write("new Method(" + m.returnType.getTypedef() + ", \"" + m.name + "\", new Class[]{");
            for (int i = 0; i < m.parameterTypes.length; i++) { 
                if (i != 0) { 
                    writer.write(", ");
                }
                writer.write(m.parameterTypes[i].getTypedef());
            }
            writer.write("}) {");
            fill(writer, indent + SerGen.TAB*2);
            writer.write("public Object invoke(Object obj, Object[] args) { ");
            if (m.throwsException) { 
                writer.write("try { ");
            }
            if (m.returnType.kind != JavaType.tpVoid) { 
                writer.write(m.returnType.kind <= JavaType.tpDouble 
                             ? ("return new " + JavaType.typeWrapper[m.returnType.kind] + "(") 
                             : "return ");
            }
            writer.write("((" + name + ")obj)." + m.name + "(");
            for (int i = 0; i < m.parameterTypes.length; i++) { 
                if (i != 0) { 
                    writer.write(", ");
                }
                writer.write(m.parameterTypes[i].kind <= JavaType.tpDouble ? ("((" + JavaType.typeWrapper[m.parameterTypes[i].kind] + ")args[" + i + "])." + JavaType.builtinType[m.parameterTypes[i].kind] + "Value()") : ("(" + m.parameterTypes[i].expr + ")args[" + i + "]"));
            }
            writer.write(m.returnType.kind != JavaType.tpVoid ? (m.returnType.kind <= JavaType.tpDouble ? ")); " : "); ") : "); return null; ");
            if (m.throwsException) { 
                writer.write(" } catch (Exception x) { throw new RuntimeException(x); } ");
            }  
            writer.write("}},");
        }   
        writer.write("}) {");
        fill(writer, indent + SerGen.TAB);
        writer.write("public Object newInstance() { return new " + name + "(); }");
        fill(writer, indent);
        writer.write("};");
    }
}

class JavaType { 
    String cls;
    String expr;
    int    kind;

    static final int tpVoid           = 0;
    static final int tpByte           = 1;
    static final int tpBoolean        = 2;
    static final int tpShort          = 3;
    static final int tpChar           = 4;
    static final int tpInt            = 5;
    static final int tpLong           = 6;
    static final int tpFloat          = 7;
    static final int tpDouble         = 8;
    static final int tpDate           = 9;
    static final int tpString         = 10;
    static final int tpObject         = 11;
    static final int tpLink           = 12;
    static final int tpArrayOfByte    = 13;
    static final int tpArrayOfBoolean = 14;
    static final int tpArrayOfShort   = 15;
    static final int tpArrayOfChar    = 16;
    static final int tpArrayOfInt     = 17;
    static final int tpArrayOfLong    = 18;
    static final int tpArrayOfFloat   = 19;
    static final int tpArrayOfDouble  = 20;
    static final int tpArrayOfDate    = 21;
    static final int tpArrayOfString  = 22;
    static final int tpArrayOfObject  = 23;
    static final int tpHashtable      = 24;
    static final int tpVector         = 25;
    static final int tpAny            = 26;

    static final String[] builtinType = {
        "void",
        "byte",
        "boolean",
        "short",
        "char",
        "int",
        "long",
        "float",
        "double"
    };

    static final String[] typeWrapper = {
        "Void",
        "Byte",
        "Boolean",
        "Short",
        "Char",
        "Integer",
        "Long",
        "Float",
        "Double"
    };

    static final String[] typeMnem = {
        "Void",
        "Byte",
        "Boolean",
        "Short",
        "Char",
        "Int",
        "Long",
        "Float",
        "Double",
        "Date",
        "String",
        "Object",
        "Link",
        "ArrayOfByte",
        "ArrayOfBoolean",
        "ArrayOfShort",
        "ArrayOfChar",
        "ArrayOfInt",
        "ArrayOfLong",
        "ArrayOfFloat",
        "ArrayOfDouble",
        "ArrayOfDate",
        "ArrayOfString",
        "ArrayOfObject",
        "Hashtable",
        "Vector",
        ""
    };        

    String getTypeName() { 
        return typeMnem[kind];
    }

    String getTypedef() { 
        return (kind == tpVoid) ? "null" : ClassReferer.getClassRef(kind <= tpDouble ? typeWrapper[kind] : expr);
    }

    String getClassName() { 
        return (kind == tpVoid) ? null : (kind <= tpDouble ? typeWrapper[kind] : expr);
    }

    JavaType(int kind, String expr) { 
        this.kind = kind;
        this.expr = expr;
    }

    JavaType(int kind, String expr, String cls) { 
        this.kind = kind;
        this.expr = expr;
        this.cls = cls;
    }
}


class JavaMethod {
    JavaType   returnType;
    String     name;
    JavaType[] parameterTypes;
    JavaMethod next;
    boolean    throwsException;
}

class JavaGeneratedMethod {
    JavaClass  cls;
    int        indentation;
    int        offset;
    int        length;
    String     param;    
    
    static final int READ_OBJECT = 1;
    static final int WRITE_OBJECT = 2;
    static final int IS_PINNABLE = 3;
    static final int GET_CLASS_DESCRIPTOR = 4;
    int        kind;

    JavaGeneratedMethod next;
}

class JavaFile { 
    char[] content;
    int    contentLength;
    
    static String newLine = "\n";

    JavaGeneratedMethod first;
    JavaGeneratedMethod last;

    void write(Writer writer) throws IOException { 
        int offs = 0;
        for (JavaGeneratedMethod m = first; m != null; m = m.next) { 
            writer.write(content, offs, m.offset - offs);
            switch (m.kind) { 
            case JavaGeneratedMethod.READ_OBJECT:
                m.cls.unpack(writer, m.param, m.indentation + SerGen.TAB);
                break;
             case JavaGeneratedMethod.WRITE_OBJECT:
                m.cls.pack(writer, m.param, m.indentation + SerGen.TAB);
                break;
             case JavaGeneratedMethod.IS_PINNABLE:
                m.cls.isPinnable(writer, m.indentation + SerGen.TAB);
                break;                
             case JavaGeneratedMethod.GET_CLASS_DESCRIPTOR:
                 m.cls.getClassDescriptor(writer,  m.indentation + SerGen.TAB);
                 break;
            }
            offs = m.offset + m.length;
        }
        writer.write(content, offs, contentLength - offs);
    }

    void addMethod(JavaGeneratedMethod m) { 
        if (first == null) { 
            first = m;
        } else { 
            last.next = m;
        }
        last = m;
    }        
}

class ParseError extends Error { 
    ParseError(String msg) { 
        super(msg);
    }
}

class JavaParser { 
    static final int tknVoid           = 0;
    static final int tknByte           = 1;
    static final int tknBoolean        = 2;
    static final int tknShort          = 3;
    static final int tknChar           = 4;
    static final int tknInt            = 5;
    static final int tknLong           = 6;
    static final int tknFloat          = 7;
    static final int tknDouble         = 8;
    static final int tknDate           = 9;
    static final int tknString         = 10;
    static final int tknObject         = 11;
    static final int tknLink           = 12;
    static final int tknStatic         = 13;
    static final int tknFinal          = 14;
    static final int tknPublic         = 15;
    static final int tknProtected      = 16;
    static final int tknPrivate        = 17;
    static final int tknTransient      = 18;
    static final int tknNative         = 19;
    static final int tknVolatile       = 20;
    static final int tknSynchronized   = 21;
    static final int tknAbstract       = 22;
    static final int tknClass          = 23;
    static final int tknInterface      = 24;
    static final int tknReadObject     = 25;
    static final int tknWriteObject    = 26;
    static final int tknIInputStream   = 27;
    static final int tknIOutputStream  = 28;
    static final int tknComma          = 29;
    static final int tknSemicolon      = 30;
    static final int tknLbr            = 31;
    static final int tknRbr            = 32;
    static final int tknLpar           = 33;
    static final int tknRpar           = 34;
    static final int tknLsqbr          = 35;
    static final int tknRsqbr          = 36;
    static final int tknStringLiteral  = 37;
    static final int tknCharLiteral    = 38;
    static final int tknNumericLiteral = 39;
    static final int tknIdent          = 40;
    static final int tknOperator       = 41;
    static final int tknAssign         = 42;
    static final int tknIsPinnable     = 43;
    static final int tknGetClassDescriptor = 44;
    static final int tknExtends        = 45;
    static final int tknImplements     = 46;
    static final int tknThrows         = 47;
    static final int tknEof            = 48;

    Hashtable symbolTable;
    Hashtable typeMap;
    
    JavaParser() { 
        symbolTable = new Hashtable();
        symbolTable.put("void", new Integer(tknVoid));
        symbolTable.put("byte", new Integer(tknByte));
        symbolTable.put("boolean", new Integer(tknBoolean));
        symbolTable.put("short", new Integer(tknShort));
        symbolTable.put("char", new Integer(tknChar));
        symbolTable.put("int", new Integer(tknInt));
        symbolTable.put("long", new Integer(tknLong));
        symbolTable.put("float", new Integer(tknFloat));
        symbolTable.put("double", new Integer(tknDouble));
        symbolTable.put("Date", new Integer(tknDate));
        symbolTable.put("String", new Integer(tknString));
        symbolTable.put("Link", new Integer(tknLink));
        symbolTable.put("java.util.Date", new Integer(tknDate));
        symbolTable.put("java.lang.String", new Integer(tknString));
        symbolTable.put("org.garret.perst.Link", new Integer(tknLink));
        symbolTable.put("static", new Integer(tknStatic));
        symbolTable.put("abstract", new Integer(tknAbstract));
        symbolTable.put("final", new Integer(tknFinal));
        symbolTable.put("public", new Integer(tknPublic));
        symbolTable.put("protected", new Integer(tknProtected));
        symbolTable.put("private", new Integer(tknPrivate));
        symbolTable.put("volatile", new Integer(tknVolatile));
        symbolTable.put("transient", new Integer(tknTransient));
        symbolTable.put("native", new Integer(tknNative));
        symbolTable.put("synchronized", new Integer(tknSynchronized));
        symbolTable.put("class", new Integer(tknClass));
        symbolTable.put("interface", new Integer(tknInterface));
        symbolTable.put("readObject", new Integer(tknReadObject));
        symbolTable.put("writeObject", new Integer(tknWriteObject));
        symbolTable.put("IInputStream", new Integer(tknIInputStream));
        symbolTable.put("IOutputStream", new Integer(tknIOutputStream));
        symbolTable.put("org.garret.perst.IInputStream", new Integer(tknIInputStream));
        symbolTable.put("org.garret.perst.IOutputStream", new Integer(tknIOutputStream));
        symbolTable.put("isPinnable", new Integer(tknIsPinnable));
        symbolTable.put("getClassDescriptor", new Integer(tknGetClassDescriptor));
        symbolTable.put("extends", new Integer(tknExtends));
        symbolTable.put("implements", new Integer(tknImplements));
        symbolTable.put("throws", new Integer(tknThrows));

        typeMap = new Hashtable();
        typeMap.put("Boolean", new Integer(JavaType.tpAny));
        typeMap.put("Integer", new Integer(JavaType.tpAny));
        typeMap.put("Character", new Integer(JavaType.tpAny));
        typeMap.put("Short", new Integer(JavaType.tpAny));
        typeMap.put("Long", new Integer(JavaType.tpAny));
        typeMap.put("Byte", new Integer(JavaType.tpAny));
        typeMap.put("Float", new Integer(JavaType.tpAny));
        typeMap.put("Double", new Integer(JavaType.tpAny));
        typeMap.put("Date", new Integer(JavaType.tpAny));
        typeMap.put("Object", new Integer(JavaType.tpAny));
        typeMap.put("Hashtable", new Integer(JavaType.tpHashtable));
        typeMap.put("Vector", new Integer(JavaType.tpVector));
    }

    char[]  content;
    int     contentLength;
    int     pos;
    int     line;
    String  ident;
    String  comment;

    int scan() { 
        char ch = '\0';
        while (true) { 
            while (true) { 
                if (pos >= contentLength) { 
                    return tknEof;
                }
                ch = content[pos++];
                if (!Character.isWhitespace(ch)) { 
                    break;
                }
                if (ch == '\n') { 
                    line += 1;
                }
            }
            switch (ch) { 
            case '=':
                return tknAssign;
            case ',':
                return tknComma;
            case ';':
                comment = null;
                return tknSemicolon;
            case '{':
                return tknLbr;
            case '}':
                comment = null;
                return tknRbr;
            case '[':
                return tknLsqbr;
            case ']':
                return tknRsqbr;
            case '(':
                return tknLpar;
            case ')':
                return tknRpar;
            case '\'':
                while (pos < contentLength && content[pos++] != '\'') {
                    if (content[pos-1] == '\\') { 
                        pos += 1;
                    }
                }
                return tknCharLiteral;
            case '"':
                 while (pos < contentLength && content[pos++] != '\"') {
                    if (content[pos-1] == '\\') { 
                        pos += 1;
                    }
                }
                return tknStringLiteral;
            case '/':
                if (pos < contentLength) { 
                    int commentStart = pos + 1;
                    switch (content[pos]) {
                    case '/':
                        while (++pos < contentLength && content[pos] != '\n');
                        comment = new String(content, commentStart, pos - commentStart);
                        line += 1;
                        pos += 1;
                        continue;
                    case '*':
                        while (++pos < contentLength-1 && !(content[pos] == '*' && content[pos+1] == '/')) { 
                            if (content[pos] == '\n') { 
                                line += 1;
                            }
                        }
                        comment = new String(content, commentStart, pos - commentStart);
                        pos += 2;
                        continue;
                    default:
                        break;
                    }
                }
                return tknOperator;
            default:
                if (Character.isJavaIdentifierStart(ch)) { 
                    int beg = pos-1;
                    while (pos < contentLength && (content[pos] == '.' || Character.isJavaIdentifierPart(content[pos]))) {
                        pos += 1;
                    }
                    ident = new String(content, beg, pos-beg);
                    Integer tkn = (Integer)symbolTable.get(ident);
                    if (tkn != null) { 
                        return tkn.intValue();
                    }
                    return tknIdent;
                } else if (Character.isDigit(ch)) { 
                    while (pos < contentLength && Character.isLetterOrDigit(content[pos])) { 
                        pos += 1;
                    }
                    return tknNumericLiteral;
                } else { 
                    return tknOperator;
                }
            }
        }
    }

    void patchMethod(JavaFile file, JavaClass cls, int kind, int paramType)
    {
        if (scan() != tknLpar) {
            error("'(' expected");
        }
        int tkn = scan();    
        if (tkn == paramType) { 
            if (paramType != tknRpar && scan() != tknIdent) { 
                error("Parameter expected");
            } else { 
                String param = ident;
                if (paramType != tknRpar && scan() != tknRpar) {
                    error("')' expected");
                }
                int offset = pos + skipForwardToNewLine();
                if (skipMethod()) { 
                    JavaGeneratedMethod m = new JavaGeneratedMethod();
                    if (offset >= pos) { // {}
                        m.offset = pos-1;
                        m.length = 0;
                        m.indentation = Integer.MIN_VALUE;
                    } else { 
                        m.offset = offset;
                        m.indentation = skipBackwardToNewLine() - 2;
                        m.length = pos - m.indentation - 2 - offset;
                        
                        if (content[offset + m.length - 1] == '\r') { 
                            m.length -= 1;
                        }
                    }
                    m.kind = kind;
                    m.param = param;
                    m.cls = cls;
                    file.addMethod(m);
                }
            }
        } else {
            skipMethod();
        }
    }

    void skipBody() 
    { 
        int balance = 1;
        do { 
            switch (scan()) { 
            case tknEof:
                error("Unexpected end of file");
            case tknLbr:
                balance += 1;
                break;
            case tknRbr:
                balance -= 1;
            }
        } while (balance != 0);
    }

    void parseModule(JavaFile file) { 
        int lex;
        while ((lex = scan()) != tknEof) {
            switch (lex) { 
            case tknClass:
            case tknInterface:
                parseClass(file);
                continue;
            }
        }
    }
    
    

    int getTypeKind(int lex) { 
        if (lex == tknIdent) { 
            int dot = ident.lastIndexOf('.');
            Object type = typeMap.get(ident.substring(dot+1));
            return (type != null) ? ((Integer)type).intValue() : JavaType.tpObject;
        }
        return lex - tknVoid;
    }

    void parseClass(JavaFile file) 
    {
        int lex;
        JavaClass cls = new JavaClass();
        if (scan() != tknIdent) { 
            error("Class name expected");
        }
        cls.name = ident;
        lex = scan();
        while (lex != tknLbr) {
            switch (lex) { 
                case tknEof:
                    error("Unexpected end of file");
                case tknExtends:
                { 
                    if (scan() != tknIdent) { 
                        error("Base class name expected");
                    }
                    cls.baseClass = ident;
                    lex = scan();
                    continue;
                }
                case tknImplements:
                { 
                    do { 
                        if (scan() != tknIdent) { 
                            error("Interface name expected");
                        }                        
                        String[] newInterfaces = new String[cls.interfaces.length + 1];
                        System.arraycopy(cls.interfaces, 0, newInterfaces, 0, cls.interfaces.length);                    
                        newInterfaces[newInterfaces.length-1] = ident;
                        cls.interfaces = newInterfaces;
                    } while ((lex = scan()) == tknComma);
                    continue;
                }
                default:
                    lex = scan();
            }
        }
        
      Members:
        while (true) { 
            boolean isSerialaziable = true;
            while (true) { 
                switch (lex = scan()) {
                case tknEof:
                    error("Unexpected end of file");
                case tknRbr:
                    return;
                case tknClass:
                case tknInterface:
                    parseClass(file);
                    continue Members;
                case tknSemicolon:
                    continue Members;
                case tknAbstract:
                case tknNative:
                case tknPublic:
                case tknPrivate:
                case tknProtected:
                case tknSynchronized:
                case tknVolatile:
                    continue;
                case tknFinal:
                case tknStatic:
                case tknTransient:
                    isSerialaziable = false;
                    continue;
                case tknLbr: // static initializer                
                    skipBody();
                    break;                
                case tknVoid:
                case tknByte:
                case tknBoolean:
                case tknShort:
                case tknChar:
                case tknInt:
                case tknLong:
                case tknFloat:
                case tknDouble:
                case tknDate:
                case tknString:
                case tknLink:
                case tknIdent:
                { 
                    int typeExprStart = pos - ident.length();
                    String typeExpr = ident;
                    int type = getTypeKind(lex);
                    String clsName = ident;
                    switch (scan()) { 
                    case tknLpar: // constructor 
                        skipMethod();
                        break;
                    case tknLsqbr:
                        type += JavaType.tpArrayOfBoolean - JavaType.tpBoolean;
                        if (scan() != tknRsqbr) { 
                            error("']' expected");
                        }
                        typeExpr = new String(content, typeExprStart, pos - typeExprStart);
                        lex = scan();
                        if (lex == tknComma || lex == tknLsqbr) { 
                            error("Multidimensional array are not supported");
                        } else if (lex != tknIdent) { 
                            error("Identifier expected");
                        }                            
                        // no break
                    case tknIdent:
                    { 
                        String field = ident;
                        while (true) { 
                            String declComment = comment;
                            switch (lex = scan()) { 
                            case tknLsqbr:
                                type += JavaType.tpArrayOfBoolean - JavaType.tpBoolean;
                                if (scan() != tknRsqbr) { 
                                    error("']' expected");
                                }
                                continue;
                            case tknAssign:
                              Initializer:
                                while (true) { 
                                    switch (lex = scan()) {
                                    case tknComma:
                                    case tknSemicolon:
                                        break Initializer;
                                    case tknLbr: // aggregate
                                        skipBody();
                                        continue;
                                    case tknEof:
                                        error("Invalid variable initialization");
                                    }
                                }
                                // no break
                            case tknComma:
                            case tknSemicolon:
                                if (isSerialaziable) { 
                                    JavaField f = new JavaField();
                                    f.name = field;
                                    f.type = new JavaType(type, typeExpr);
                                    f.cls = clsName;
                                    f.comment = declComment;
                                    if (declComment != null) { 
                                        if (declComment.indexOf("@serializable") >= 0) { 
                                            f.flags |= JavaField.SERIALIZABLE;
                                        } else if (declComment.indexOf("@property") >= 0) { 
                                            f.flags |= JavaField.PROPERTY;
                                        } else if (declComment.indexOf("@binary") >= 0) { 
                                            f.flags |= JavaField.BINARY;
                                            f.type = new JavaType(JavaType.tpArrayOfByte, "byte[]", typeExpr);
                                        }
                                    }
                                    cls.addField(f);
                                }
                                if (lex == tknComma) { 
                                    if (scan() != tknIdent) { 
                                        error("Field name expected");
                                    }
                                    continue;
                                }
                                break;
                            case tknLpar:
                                cls.addMethod(parseMethod(new JavaType(type, typeExpr), field));                               
                                break;
                            }
                            break;
                        }
                        break;
                    }
                    case tknReadObject:
                        if (isGenerated()) { 
                            patchMethod(file, cls, JavaGeneratedMethod.READ_OBJECT, tknIInputStream);
                        } else {
                            skipMethod();
                        }
                        break;
                    case tknWriteObject:
                        if (isGenerated()) { 
                            patchMethod(file, cls, JavaGeneratedMethod.WRITE_OBJECT, tknIOutputStream);
                        } else {
                            skipMethod();
                        }
                        break;
                    case tknIsPinnable:
                        if (isGenerated()) { 
                            patchMethod(file, cls, JavaGeneratedMethod.IS_PINNABLE, tknRpar);
                        } else {
                            skipMethod();
                        }
                        break;
                    case tknGetClassDescriptor:
                        if (isGenerated()) { 
                            patchMethod(file, cls, JavaGeneratedMethod.GET_CLASS_DESCRIPTOR, tknRpar);
                        } else {
                            skipMethod();
                        }
                        break;
                    default:
                        error("Field or method name expected");
                    } 
                    break;
                }
                default:
                    error("Field or method type expected");
                }
                break;
            }
        }
    }

    final boolean isGenerated() { 
        return comment == null || comment.indexOf("@sealed") < 0;
    }

    int skipForwardToNewLine() 
    {
        int p;
        for (p = pos; p < contentLength && content[p] != '\n' && content[p] != '\r'; p++);
        return p - pos;
    }

    int skipBackwardToNewLine() 
    {
        int p;
        for (p = pos; --p >= 0 && content[p] != '\n';);
        return pos - p;
    }

    void error(String msg) { 
        throw new ParseError(msg);
    }

    JavaMethod parseMethod(JavaType returnType, String name) { 
        JavaMethod method = new JavaMethod();
        method.name = name;
        method.returnType = returnType;
        method.parameterTypes = new JavaType[0];
        int lex;
        while ((lex = scan()) != tknRpar) { 
            switch (lex) { 
            case tknComma:
                continue;
            case tknVoid:
            case tknByte:
            case tknBoolean:
            case tknShort:
            case tknChar:
            case tknInt:
            case tknLong:
            case tknFloat:
            case tknDouble:
            case tknDate:
            case tknString:
            case tknLink:
            case tknIInputStream:
            case tknIOutputStream:
            case tknIdent:
            {
                int typeExprBeg = pos - ident.length();
                int typeExprEnd = pos;
                int typeKind = getTypeKind(lex);
                while ((lex = scan()) == tknLsqbr) { 
                    while ((lex = scan()) != tknRsqbr) { 
                        if (lex == tknEof) { 
                            error("']' expected");
                        }
                    }
                    typeExprEnd = pos;
                }
                if (lex != tknIdent) { 
                    error("Parameter name expected");
                }
                JavaType[] newParameterTypes = new JavaType[method.parameterTypes.length + 1];
                System.arraycopy(method.parameterTypes, 0, newParameterTypes, 0, method.parameterTypes.length);                
                newParameterTypes[newParameterTypes.length-1] = new JavaType(typeKind, new String(content, typeExprBeg, typeExprEnd - typeExprBeg));
                method.parameterTypes = newParameterTypes;
                continue;
            }
            default:
                error("Type expected");
            }
        }
        if ((lex = scan()) == tknThrows) { 
            method.throwsException = true;
        }
        while (lex != tknLbr && lex != tknSemicolon) {
            if (lex == tknEof) { 
                error("Unexpected end of file");
            }
            lex = scan();
        }
        if (lex == tknLbr) { 
            skipBody();
        }
        return method;
    }

       
    boolean skipMethod() {
        int lex;
        while ((lex = scan()) != tknLbr && lex != tknSemicolon) {
            if (lex == tknEof) { 
                error("Unexpected end of file");
            }
        }
        if (lex == tknLbr) { 
            skipBody();
            return true;
        }
        return false;
    }

    void parse(JavaFile f) { 
        content = f.content;
        contentLength = f.contentLength;
        for (int i = 0; i < contentLength; i++) {
            if (content[i] == '\n') { 
                JavaFile.newLine = (i > 0 && content[i-1] == '\r') ? "\r\n" : "\n";
                break;
            }
        }
        pos = 0;
        line = 1;
        parseModule(f);
    }
}
    
public class SerGen 
{ 
    static final int TAB = 4;

    static void processFile(JavaParser parser, File file) throws IOException
    { 
        if (file.isDirectory()) {
            String[] list = file.list();
            for (int i = 0; i < list.length; i++) { 
                File f = new File(file, list[i]);
                if (f.isDirectory() || list[i].endsWith(".java")) { 
                    processFile(parser, f);
                }
            }
        } else { 
            FileReader reader = new FileReader(file);
            int fileLength = (int)file.length();
            char[] buf = new char[fileLength];
            int offs = 0, rc;
            while ((rc = reader.read(buf, offs, fileLength - offs)) > 0) { 
                offs += rc;
            }
            reader.close();
            JavaFile f = new JavaFile();
            f.content = buf;
            f.contentLength = offs;
            try { 
                parser.parse(f);
            } catch (ParseError e) { 
                System.err.println("Error in file " + file.getPath() + " at line " + parser.line + ": " + e.getMessage());
            }
            if (f.first != null) { 
                File tmp = new File(file.getPath() + ".tmp");
                FileWriter writer = new FileWriter(tmp);
                f.write(writer);
                writer.close();
                file.delete();
                if (!tmp.renameTo(file)) { 
                    System.err.println("Failed to rename file to " + file.getPath());
                } else { 
                    System.out.println("Patch file " + file.getPath());
                }
            }
        }
    }

    public static void main(String args[]) throws IOException { 
        JavaParser parser = new JavaParser();            
        for (int i = 0; i < args.length; i++) { 
            if (args[i].equals("-blackberry")) { 
                ClassReferer.blackberryVerificationBugWorkarround = true;
            } else { 
                processFile(parser, new File(args[i]));
            }
        }
    }
}
