import org.garret.perst.*;

class SpatialObject extends Persistent { 
    Rectangle rect;

    public String toString() { 
        return rect.toString();
    }
}

public class TestRtree extends Persistent { 
    SpatialIndex<SpatialObject> index;
    final static int nObjectsInTree = 1000;
    final static int nIterations = 100000;

    public static void main(String[] args) { 
        Storage db = StorageFactory.getInstance().createStorage();
        long start = System.currentTimeMillis();
        db.open("testrtree.dbs");
        TestRtree root = (TestRtree)db.getRoot();
        if (root == null) { 
            root = new TestRtree();
            root.index = db.<SpatialObject>createSpatialIndex();
            db.setRoot(root);
        }

        Rectangle[] rectangles = new Rectangle[nObjectsInTree];
        long key = 1999;
        for (int i = 0; i < nIterations; i++) { 
            int j = i % nObjectsInTree;
            if (i >= nObjectsInTree) { 
                Rectangle r = rectangles[j];
                SpatialObject po = null;
                int n = 0;
                for (SpatialObject so:root.index.iterator(r))
                {
                    if (r.equals(so.rect)) { 
                        po = so;
                    } else { 
                        Assert.that(r.intersects(so.rect));
                    }
                    n += 1;
                }    
                Assert.that(po != null);
                for (int k = 0; k < nObjectsInTree; k++) { 
                    if (r.intersects(rectangles[k])) {
                        n -= 1;
                    }
                }
                Assert.that(n == 0);
                root.index.remove(r, po);
                po.deallocate();
            }
            key = (3141592621L*key + 2718281829L) % 1000000007L;
            int top = (int)(key % 1000);
            int left = (int)(key / 1000 % 1000);            
            key = (3141592621L*key + 2718281829L) % 1000000007L;
            int bottom = top + (int)(key % 100);
            int right = left + (int)(key / 100 % 100);
            SpatialObject so = new SpatialObject();
            Rectangle r = new Rectangle(top, left, bottom, right);
            so.rect = r;
            rectangles[j] = r;
            root.index.put(r, so);

            if (i % 100 == 0) { 
                double minDistance = 0;
                j = 0;
                for (SpatialObject obj:root.index.neighborIterator(0, 0)) {
                    double distance = Math.sqrt(obj.rect.getLeft()*obj.rect.getLeft() + obj.rect.getTop()*obj.rect.getTop());
                    Assert.that(distance >= minDistance);
                    minDistance = distance;
                    j += 1;
                }
                Assert.that((i >= nObjectsInTree && j == nObjectsInTree) || (i < nObjectsInTree && j == i+1));
                System.out.print("Iteration " + i + "\r");
                System.out.flush();
                db.commit();
            }
        }        
        root.index.clear();
        System.out.println("\nElapsed time " + (System.currentTimeMillis() - start));
        db.close();
    }
}
