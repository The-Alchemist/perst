import org.garret.perst.*;

import java.util.*;

public class TestLeak 
{
    static class SpatialObject extends Persistent implements SelfSerializable
    {
        RectangleR2 rect;
        byte[] body;

        public void pack(PerstOutputStream out) throws java.io.IOException
        {
            out.writeDouble(rect.getTop());
            out.writeDouble(rect.getLeft());
            out.writeDouble(rect.getBottom());
            out.writeDouble(rect.getRight());
            out.writeInt(body.length);
            out.write(body, 0, body.length);
        }

        public  void unpack(PerstInputStream in) throws java.io.IOException
        {
            rect = new RectangleR2(in.readDouble(), in.readDouble(), in.readDouble(), in.readDouble());
            body = new byte[in.readInt()];
            in.read(body);
        }
                
    };


    static final int nObjects = 1000;
    static final int batchSize = 100;
    static final int minObjectSize = 1000;
    static final int maxObjectSize = 2000;
    static final int nIterations = 10000;

    public static void main(String[] args) throws Exception { 
        Storage db = StorageFactory.getInstance().createStorage();
        db.open("testleak.dbs");
        SpatialIndexR2<SpatialObject> root = db.<SpatialObject>createSpatialIndexR2();
        RectangleR2[] rectangles = new RectangleR2[nObjects];
        Random rnd = new Random(2014);
        db.setRoot(root);
        for (int i = 0; i < nObjects; i++) { 
            SpatialObject so = new SpatialObject();
            double lat = rnd.nextDouble()*180;
            double lng = rnd.nextDouble()*180;
            so.rect = rectangles[i] = new RectangleR2(lat, lng, lat+10, lng+10);
            so.body = new byte[minObjectSize + rnd.nextInt(maxObjectSize - minObjectSize)];
            root.put(so.rect, so);
        } 
        db.commit();

        for (int i = 0; i < nIterations; i++) {
            if (i % 1000 == 0) { 
                System.out.println("Iteration " + i);
            }
            for (int j = 0; j < batchSize; j++) { 
                int k = rnd.nextInt(nObjects);
                boolean found = false;
                for (SpatialObject oldObj : root.iterator(rectangles[k])) { 
                    if (oldObj.rect.equals(rectangles[k])) {
                        root.remove(oldObj.rect, oldObj);
                        SpatialObject newObj = new SpatialObject();
                        newObj.rect = oldObj.rect;
                        newObj.body = new byte[minObjectSize + rnd.nextInt(maxObjectSize - minObjectSize)];
                        root.put(newObj.rect, newObj);
                        oldObj.deallocate();
                        found = true;
                        break;
                    }
                }
                Assert.that(found);
            }
            db.commit();
        }
        db.close();
    }
}

