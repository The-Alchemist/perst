package org.garret.rdf.xml;

import org.xml.sax.*;
import org.xml.sax.helpers.DefaultHandler;
import javax.xml.parsers.*;
import java.util.*;
import java.text.*;
import java.io.*;
import org.garret.rdf.*;

/**
 * Parse XML requests and store/fetch data in storage
 */
public class XmlServer {
    VersionedStorage store;
    PrintStream      writer;

    public static String[] dateFormats = {  
        "EEE, d MMM yyyy kk:mm:ss z",
        "EEE, d MMM yyyy kk:mm:ss",
        "yyyy-MM-dd HH:mm:ss",
        "yyyy-MM-dd HH:mm",
        "yyyy-MM-dd"
    };
    public static DateFormat[] dateFormatters;

    static { 
        dateFormatters = new DateFormat[dateFormats.length];
        for (int i = 0; i < dateFormatters.length; i++) { 
            dateFormatters[i] = new SimpleDateFormat(dateFormats[i], Locale.ENGLISH);
        }
    }

    public XmlServer(VersionedStorage store, PrintStream writer) 
    {
        this.store = store;
        this.writer = writer;
    }

    static class RdfResource { 
        RdfResource outer;
        ArrayList   props;
        Object      value;
        String      uri;
        
        RdfResource(RdfResource outer) { 
            this.outer = outer;
            props = new ArrayList();
        }
    }
                
    static class RdfPattern { 
        RdfPattern  outer;
        ArrayList   props;
        Object      value;
        String      uri;
        int         depth;
        SearchKind  kind;
        Date        timestamp;
        
        RdfPattern(RdfPattern outer) { 
            this.outer = outer;
            props = new ArrayList();
            kind = SearchKind.LatestVersion;
        }
    }

    DefaultHandler getHandler() { 
        return new RdfHandler();
    }

    class RdfHandler extends DefaultHandler { 
        DefaultHandler handler;

        RdfHandler() { 
            handler = new RdfDocumentParser();
        }

        public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException { 
            handler.startElement(uri, localName, qName, attributes);
        }

        public void endElement(String uri, String localName, String qName) throws SAXException { 
            handler.endElement(uri, localName, qName);
        }

        public void characters(char ch[], int start, int length) throws SAXException { 
            handler.characters(ch, start, length);
        }

        class RdfDocumentParser extends DefaultHandler { 
            public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException { 
                String name = uri + localName;
                if (XmlSymbols.Store.equals(name)) { 
                    handler = new RdfStoreHandler(this);
                } else if (XmlSymbols.Find.equals(name)) { 
                    handler = new RdfFindHandler(this);
                } else { 
                    throw new SAXException("Unknown operation '" + name + "'");
                }
            }
        }
        
        class RdfStoreHandler extends DefaultHandler { 
            RdfResource curr;
            DefaultHandler prevHandler;
            
            RdfStoreHandler(DefaultHandler prevHandler) { 
                curr = new RdfResource(null);
                this.prevHandler = prevHandler;
                store.beginTransaction();
            }

            public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
                RdfResource res = new RdfResource(curr);
                for (int i = 0, n = attributes.getLength(); i < n; i++) { 
                    String name = attributes.getURI(i) + attributes.getLocalName(i);
                    String value = attributes.getValue(i);
                    if (XmlSymbols.Uri.equals(name)) {
                        res.uri = value;
                    } else if (XmlSymbols.Ref.equals(name)) {
                        if ((res.value = store.getObject(value)) == null) { 
                            throw new SAXException("Object with URI '" + value + "' is not found");
                        }
                    } else { 
                        res.props.add(createProperty(name, value));
                    }
                }
                if (res.uri == null) { 
                    String parentURI = curr.uri == null 
                        ? XmlSymbols.NS + new java.rmi.server.UID()
                        : curr.uri;
                    res.uri = parentURI + '/' + localName;
                }
                curr = res;
            }

            public void characters (char ch[], int start, int length) {
                if (curr != null && length != 0) { 
                    curr.value = convertValue(new String(ch, start, length));
                }
            }
            
            public void endElement (String uri, String localName, String qName) {
                if (curr.outer == null) { 
                    store.commit();
                    handler = prevHandler;
                } else {
                    String name = uri+localName;
                    Object value;
                    if (curr.props.size() != 0) {
                        Thing thing = store.createObject(curr.uri, name, (NameVal[])curr.props.toArray(new NameVal[curr.props.size()]));
                        value = thing.vh;
                    } else { 
                        value = curr.value;
                    }
                    curr = curr.outer;
                    curr.props.add(new NameVal(name, value));
                }
            }

            private NameVal createProperty(String name, String value) throws SAXException  {
                if (XmlSymbols.Subtype.equals(name)) {
                    VersionHistory vh = store.getObject(value);
                    if (vh == null) { 
                        throw new SAXException("Object with URI '" + value + "' is not found");
                    }
                    return new NameVal(name, vh);
                } else {         
                    return new NameVal(name, convertValue(value));
                }
            }
        }

        class RdfFindHandler extends DefaultHandler { 
            RdfPattern curr;
            DefaultHandler prevHandler;
            
            RdfFindHandler(DefaultHandler prevHandler) { 
                this.prevHandler = prevHandler;
                writer.println("<?xml version='1.0'?>");
                writer.println("<vr:result xmlns:vr=\"http://www.perst.org#\" xmlns:rdfs=\"http://www.w3.org/2000/01/rdf-schema#\" xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\">");
            }

            public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
                RdfPattern pattern = new RdfPattern(curr);
                for (int i = 0, n = attributes.getLength(); i < n; i++) { 
                    String name = attributes.getURI(i) + attributes.getLocalName(i);
                    String value = attributes.getValue(i);
                    if (XmlSymbols.Uri.equals(name)) { 
                        pattern.uri = value;
                    } else if (XmlSymbols.Before.equals(name)) { 
                        if ((pattern.timestamp = parseDate(value)) == null) { 
                            throw new SAXException("Invalid date format " + value);
                        }
                        pattern.kind = SearchKind.LatestBefore;
                    } else if (XmlSymbols.After.equals(name)) { 
                        if ((pattern.timestamp = parseDate(value)) == null) { 
                            throw new SAXException("Invalid date format " + value);
                        }
                        pattern.kind = SearchKind.OldestAfter;
                    } else if (XmlSymbols.All.equals(name)) { 
                        pattern.kind = Boolean.valueOf(value).booleanValue() 
                            ? SearchKind.AllVersions : SearchKind.LatestVersion;
                    } else if (XmlSymbols.Depth.equals(name)) { 
                        pattern.depth = Integer.parseInt(value, 10);
                    } else { 
                        pattern.props.add(new NameVal(name, convertPattern(value)));
                    }
                }
                curr = pattern;
            }

            public void characters (char ch[], int start, int length) { 
                if (curr != null && length != 0) { 
                    curr.value = convertPattern(new String(ch, start, length));
                }
            }
            
            public void endElement (String uri, String localName, String qName) {
                if (curr == null) { 
                    writer.println("</vr:result>");
                    handler = prevHandler;
                } else {
                    String name = uri+localName;
                    if (curr.outer == null) { 
                        String type = XmlSymbols.Thing.equals(name) ? null : name;
                        Iterator iterator = store.search(type,
                                                         curr.uri, 
                                                         (NameVal[])curr.props.toArray(new NameVal[curr.props.size()]),
                                                         curr.kind, 
                                                         curr.timestamp);
                        while (iterator.hasNext()) { 
                            Thing thing = (Thing)iterator.next();
                            dumpObject(thing, writer, 1, curr.kind, curr.timestamp, curr.depth);
                        }
                    } else { 
                        Object value;
                        if (curr.props.size() != 0) {
                            value = curr.props.toArray(new NameVal[curr.props.size()]);
                        } else { 
                            value = curr.value;
                        }
                        curr.outer.props.add(new NameVal(name, value));
                    }
                    curr = curr.outer;
                }
            }
        }
    }
             
    private static Object convertValue(String str) 
    {
        if (str.length() > 0) 
        {
            char ch = str.charAt(0);
            if (ch == '+' || ch == '-' || (ch >= '0' && ch <= '9')) {
                try { 
                    return new Double(Double.parseDouble(str));
                } catch (NumberFormatException x) {}
                Date date = parseDate(str);
                if (date != null) { 
                    return date;
                }
            }
        }
        return str;
    }

    private static Object convertPattern(String str) 
    {
        int comma;
        int len = str.length();
        if (len > 3 && (str.charAt(0) == '[' || str.charAt(0) == '(')
            && (str.charAt(len-1) == ']' || str.charAt(len-1) == ')') 
            && (comma = str.indexOf(',', 1)) > 0)
        {
            String from = str.substring(1, comma).trim();
            Object fromVal = convertValue(from);
            boolean fromInclusive = str.charAt(0) == '[';
            String till = str.substring(comma+1, len-1).trim();
            Object tillVal = convertValue(till);
            boolean tillInclusive = str.charAt(len-1) == ']';
            if (!fromVal.getClass().equals(tillVal.getClass())) {
                fromVal = from;
                tillVal = till;
            }
            return new Range(fromVal, fromInclusive, tillVal, tillInclusive);
        }
        return convertValue(str);
    }
   
    static Date parseDate(String str) { 
        for (int i = 0; i < dateFormatters.length; i++) { 
            ParsePosition pos = new ParsePosition(0);
            Date date = dateFormatters[i].parse(str, pos);
            if (date != null && pos.getIndex() == str.length()) { 
                return date;
            }
        }
        return null;
    }

    static String getQualifiedName(String uri, PrintStream writer) {
        int col = uri.lastIndexOf(':');
        int hash = uri.lastIndexOf('#');
        int path = uri.lastIndexOf('/');
        int namespaceLen = Math.max(Math.max(col, hash), path);
                       
        writer.print('<');
        if (namespaceLen > 0) { 
            String prefix = uri.substring(0, namespaceLen+1);
            String name = uri.substring(namespaceLen+1);
            if (prefix.equals(Symbols.RDF)) { 
                name = "rdf:" + name;
            } else if (prefix.equals(Symbols.RDFS)) {
                name = "rdfs:" + name;
            } else if (prefix.equals(Symbols.NS)) {
                name = "vr:" + name;
            } else { 
                writer.print(name + " xmls=\"" + prefix + "\"");
                return name;
            } 
            uri = name;
        }
        writer.print(uri);
        return uri;
    }

    static void dumpObject(Thing thing, PrintStream writer, int indent, SearchKind kind, Date timestamp, int depth) {     
        writeTab(writer, indent);
        String typeName = getQualifiedName(thing.type.vh.uri, writer);  
        writer.println(" rdf:about=\"" + thing.vh.uri + "\" vr:timestamp=\"" + thing.timestamp + "\">");
        for (int i = 0; i < thing.props.length; i++) { 
            PropVal pv =  thing.props[i];
            Object val = pv.val;
            if (val instanceof VersionHistory) {
                VersionHistory ptr = (VersionHistory)val;
                if (kind != SearchKind.AllVersions) {
                    if (depth > 0 || ptr.uri.startsWith(thing.vh.uri)) { 
                        Thing t = ptr.getVersion(kind, timestamp);
                        if (t != null) {
                            dumpObject(t, writer, indent+1, kind, timestamp, depth-1);
                            continue;
                        }
                    }
                }
                writeTab(writer, indent+1);
                getQualifiedName(pv.def.name, writer);
                writer.println(" rdf:resource=\"" + ptr.uri + "\"/>");
            } else { 
                writeTab(writer, indent+1);
                String propName = getQualifiedName(pv.def.name, writer);
                writer.println(">" + val + "</" + propName + ">");
            }
        }
        writeTab(writer, indent);
        writer.println("</" + typeName + ">");
    }

    static void writeTab(PrintStream writer, int ident) { 
        while (--ident >= 0) writer.print('\t');
    }


    public static void main(String[] args) throws Exception
    {
        if (args.length < 2) 
        { 
            System.err.println("Usage: ImportXML database-file xml-file {xml-file}");
            return;
        }
        VersionedStorage store = new VersionedStorage();
        store.open(args[0]);
        XmlServer server = new XmlServer(store, System.out);
        SAXParserFactory factory = SAXParserFactory.newInstance();
        factory.setNamespaceAware(true);
        factory.setValidating(false);
        for (int i = 1; i < args.length; i++) 
        { 
            SAXParser parser = factory.newSAXParser();
            parser.parse(new File(args[i]), server.getHandler()); 
        }
        store.close();
        System.out.println("Press any key to exit...");
        System.in.read();
    }
}
