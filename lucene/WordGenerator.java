public class WordGenerator { 
    static final int  MAX_WORDS = 10000;
    static final long INIT_VALUE = 1999; 
    int  n;
    long v;

    WordGenerator() { 
        v = INIT_VALUE;
        n = 0;
    }
        
    void generateWord(StringBuffer buf) { 
        if (++n == MAX_WORDS) { 
            v = INIT_VALUE;
            n = 0;
        }
        v = (3141592621L*v + 2718281829L) % 1000000007L;
        long w = v;
        do { 
            buf.append((char)('a' + (w & 15)));
        } while ((w >>>= 4) != 0);
    }
}

