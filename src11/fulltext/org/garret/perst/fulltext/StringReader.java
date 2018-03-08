package org.garret.perst.fulltext;

import java.io.Reader;

/**
 * Implementation of string reader for J2ME
 */
public class StringReader extends Reader
{
    public void close() 
    {
        str = null;
    }

    public int read()
    {
        return (currPos < str.length()) ? str.charAt(currPos++) : -1;
    }

    public int read(char[] cbuf, int off, int len)
    {
        int rest = str.length() - currPos;
        if (rest <= 0) { 
            return -1;
        }
        if (len > rest) { 
            len = rest;
        }
        str.getChars(currPos, currPos + len, cbuf, 0);
        currPos += len;
        return len;
    }
    
    public long skip(long n) 
    {
        return currPos += n;
    }
    
    public StringReader(String str) {
        this.str = str;
    }

    int currPos;
    String str;
};
