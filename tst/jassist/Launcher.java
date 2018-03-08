import javassist.*;
import org.garret.perst.jassist.PerstTranslator;

public class Launcher { 
    public static void main(String[] args) throws Throwable { 
        if (args.length < 1) {
            System.err.println("Usage: java Laucher CLASS-NAME arguments...");
        } else { 
            Translator trans = new PerstTranslator();
            ClassPool pool = ClassPool.getDefault();
            Loader cl = new Loader(pool);
            cl.addTranslator(pool, trans);
            Thread.currentThread().setContextClassLoader(cl);
            String[] params = new String[args.length-1];
            System.arraycopy(args, 1, params, 0, params.length);
            cl.run(args[0], params);
        }
    }
}