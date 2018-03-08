import java.util.*;
import java.io.*;
import org.garret.perst.*;
import org.garret.perst.fulltext.*;
import javax.microedition.pim.*;

public class ContactDetails extends Persistent implements FullTextSearchable
{
    static class Pair { 
        String label;
        String value;

        Pair(String label, String value) { 
            this.label = label;
            this.value = value;
        }
    }
    Vector pairs;
   
    public Reader getText()
    {
        StringBuffer buf = new StringBuffer();
        int nPairs = pairs.size();
        for (int i = 0; i < nPairs; i++) { 
            Pair pair = (Pair)pairs.elementAt(i);
            buf.append(pair.value);
            buf.append('\n');
        }
        return new StringReader(buf.toString());
    }

    public String getLanguage() { 
        return PIMindex.LANGUAGE;
    }

    // Deserialize the object
    public void readObject(IInputStream in) { 
        int nPairs = in.readInt();
        pairs = new Vector(nPairs);
        for (int i = 0; i < nPairs; i++) { 
            Pair pair = new Pair(in.readString(), in.readString());
            pairs.addElement(pair);
        }
    }

    // Serialize the object
    public void writeObject(IOutputStream out) { 
        int nPairs = pairs.size();
        out.writeInt(nPairs);
        for (int i = 0; i < nPairs; i++) { 
            Pair pair = (Pair)pairs.elementAt(i);
            out.writeString(pair.label);
            out.writeString(pair.value);
        }
    }

    public String buildSnippet(String query, FullTextSearchHelper helper) { 
        String best = "";
        boolean exact = false;
        query = query.toLowerCase();
        int nPairs = pairs.size();
        for (int i = 0; i < nPairs; i++) { 
            Pair pair = (Pair)pairs.elementAt(i);
            String snippet = pair.value;
            int pos = snippet.toLowerCase().indexOf(query);
            if (pos >= 0) { 
                if ((pos == 0 || !helper.isWordChar(snippet.charAt(pos-1))) 
                    && (pos+query.length() == snippet.length() || !helper.isWordChar(snippet.charAt(pos+query.length()))))
                {
                    if (!exact) { 
                        exact = true;
                        best = snippet;
                    } else if (snippet.length() > best.length()) { 
                        best = snippet;
                    }
                } else if (!exact && snippet.length() > best.length()) { 
                    best = snippet;
                }
            }
        }    
        if (best.length() == 0 && nPairs > 0) { 
            Pair pair = (Pair)pairs.elementAt(0);
            best = pair.value;
        }
        return best;
    }

    
    static String generateLabel(String label, int attributes, PIMList list) 
    { 
        StringBuffer buf = new StringBuffer();
        buf.append(label);
        char sep = '(';
        for (int k = 1; attributes != 0; k <<= 1) { 
            if ((attributes & k) != 0) { 
                attributes -= k;
                buf.append(sep);
                sep = ',';
                buf.append(list.getAttributeLabel(k));
            }
        }
        if (sep != '(') { 
            buf.append(')');
        }
        return buf.toString();
    }                  

    public ContactDetails(Contact contact) {
        int[] fields = contact.getFields();
        PIMList list = contact.getPIMList();
        pairs = new Vector();        
        for (int i = 0; i < fields.length; i++) {
            int field = fields[i]; 
            String label = list.getFieldLabel(field);
            int count = contact.countValues(field);
            int[] supportedAttributes = list.getSupportedAttributes(field);
            for (int j = 0; j < count; j++) { 
                int attributes = contact.getAttributes(field, j);
                switch (list.getFieldDataType(field)) {
                case PIMItem.STRING:
                {
                    String value = contact.getString(field, j);                    
                    if (value != null && value.length() > 0) { 
                        pairs.addElement(new Pair(generateLabel(label, attributes, list), value));
                    }
                    break;
                }
                case PIMItem.STRING_ARRAY:
                {
                    String[] values = contact.getStringArray(field, j);
                    for (int k = 0; k < values.length; k++) { 
                        String value = values[k];
                        if (value != null && value.length() > 0) { 
                            pairs.addElement(new Pair(generateLabel(label, attributes, list), value));
                        }
                    }
                    break;
                }                   
                case PIMItem.DATE:
                {
                    long time = contact.getDate(field, j);
                    if (time != -1) { 
                        pairs.addElement(new Pair(generateLabel(label, attributes, list), 
                                                  new Date(time).toString()));
                    }
                    break;
                }
                case PIMItem.BOOLEAN:
                    pairs.addElement(new Pair(generateLabel(label, attributes, list), 
                                              contact.getBoolean(field, j) ? "true" : "false"));  
                    break;
                case PIMItem.INT:
                    pairs.addElement(new Pair(generateLabel(label, attributes, list), 
                                              Integer.toString(contact.getInt(field, j))));  
                    break;
                }
            }
        }
    }

    public ContactDetails() {}
}
