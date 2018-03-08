import java.io.*;
import java.util.zip.*;

/**
 * Encypt/decrypt/compress/decompress existed database file
 */
public class DBConv
{
    static final int PAGE_SIZE = 4096;
    static final int COMPRESS = 1;
    static final int DECOMPRESS = 2;
    static final int CRYPT = 4;

    private final void crypt(byte[] buf, int len)
    {
        int x = 0, y = 0;
        byte[] state = this.state;
        System.arraycopy(initState, 0, state, 0, state.length);
	for (int i = 0; i < len; i++) {
            x = (x + 1) & 0xff;
            y = (y + state[x]) & 0xff;
            byte temp = state[x];
            state[x] = state[y];
            state[y] = temp;
            int nextState = (state[x] + state[y]) & 0xff;
	    buf[i] ^= state[nextState];
        }
    }
    
    private final int compress(byte[] out, byte[] in, int length) 
    { 
        Deflater compresser = new Deflater();
        compresser.setInput(in, 0, length);
        compresser.finish();
        return compresser.deflate(out);
    }

    private final int decompress(byte[] out, byte[] in, int length) throws DataFormatException
    { 
        Inflater decompresser = new Inflater();
        decompresser.setInput(in, 0, length);
        int resultLength = decompresser.inflate(out);
        decompresser.end();
        return resultLength;
    }

    public void setKey(String cipherKey) 
    { 
        byte[] key = cipherKey.getBytes();

        initState = new byte[256];
        state = new byte[256];

	for (int counter = 0; counter < 256; ++counter) { 
	    initState[counter] = (byte)counter;
        }
	int index1 = 0;
	int index2 = 0;
	for (int counter = 0; counter < 256; ++counter) {
	    index2 = (key[index1] + initState[counter] + index2) & 0xff;
	    byte temp = initState[counter];
	    initState[counter] = initState[index2];
	    initState[index2] = temp;
	    index1 = (index1 + 1) % key.length;
        }
    }

    public void proceed(String filePath) throws Exception
    {
        File tmp = new File(filePath + ".tmp");
        File orig = new File(filePath + ".orig");
        File dst = new File(filePath);
        FileOutputStream out = new FileOutputStream(tmp);
        FileInputStream in = new FileInputStream(dst);
        byte[] page = new byte[PAGE_SIZE];
        byte[] buf = new byte[PAGE_SIZE];
        int rc;
        if ((mode & DECOMPRESS) != 0) { 
            DataInputStream din = new DataInputStream(in);
            while (true) { 
                int size = din.readUnsignedShort();
                int offs = 0;
                while (offs < size && (rc = din.read(page, offs, size - offs)) > 0) { 
                    offs += rc;
                }
                if (offs <= 0) {
                    break;
                } else if (offs != size) { 
                    throw new Error("File is corrupted");
                }
                if ((mode & CRYPT) != 0) { 
                    crypt(page, size);
                }
                rc = decompress(buf, page, size);
                out.write(buf, 0, rc);
            }
        } else {             
            DataOutputStream dout = new DataOutputStream(out);
            while ((rc = in.read(page, 0, PAGE_SIZE)) > 0) { 
                if ((mode & COMPRESS) != 0) { 
                    rc = compress(page, page, rc);
                    dout.writeShort(rc);
                }                    
                if ((mode & CRYPT) != 0) { 
                    crypt(page, rc);
                }
                dout.write(page, 0, rc);
            }
            dout.flush();
        }
        in.close();
        out.close();
        if (!dst.renameTo(orig)) {
            System.err.println("Failed to rename original file");
        }
        if (!tmp.renameTo(dst)) {
            System.err.println("Failed to rename generated file");
        }
    }
          
        
    public static void main(String[] args) throws Exception 
    { 
        if (args.length < 2) { 
            System.err.println("Database compression/encryption utility");
            System.err.println("Usage: java -classpath dbconv.jar DBConv [-encrypt KEY | -decrypt KEY] [-inflate|-deflate] DATABASE-FILE(s)");
            return;
        }
        DBConv conv = new DBConv();
        for (int i = 0; i < args.length; i++) { 
            String arg = args[i];
            if (arg.equals("-encrypt") || arg.equals("-decrypt")) { 
                conv.setKey(args[++i]);
                conv.mode |= CRYPT;
            } else if (arg.equals("-plaintext")) { 
                conv.mode &= ~CRYPT;                
            } else if (arg.equals("-deflate") || arg.equals("-compress") || arg.equals("-zip")) {
                conv.mode &= ~DECOMPRESS;
                conv.mode |= COMPRESS;
            } else if (arg.equals("-inflate") || arg.equals("-decompress") || arg.equals("-unzip")) {
                conv.mode |= DECOMPRESS;
                conv.mode &= ~COMPRESS;
            } else if (arg.startsWith("-")) { 
                System.err.println("Unknown option: " + arg);
                System.err.println("Usage: java -classpath dbconv.jar DBConv [-encrypt KEY | -decrypt KEY] [-inflate|-deflate] DATABASE-FILE(s)");
                return;                
            } else { 
                conv.proceed(arg);
            }
        }
    }

    byte[] initState;
    byte[] state;
    int mode;
}