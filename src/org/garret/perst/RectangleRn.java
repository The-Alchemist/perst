package org.garret.perst;

/**
 * R-n rectangle class representing n-dimensional wrapping rectangle. This class is used in spatial index.
 */
public class RectangleRn implements IValue, Cloneable 
{
    double[] coords;

    /**
     * Get N - number of dimensions
     */
    public int nDimensions() { 
        return coords.length/2;
    }
    
    /**
     * Get minimal value for i-th coordinate of rectangle 
     */
    public final double getMinCoord(int i) { 
        return coords[i];
    }

    /**
     * Get maximal value for i-th coordinate of rectangle 
     */
    public final double getMaxCoord(int i) { 
        return coords[coords.length/2 + i];
    }

    /**
     * Rectangle area
     */
    public final double area() { 
        double a = 1.0;
        for (int i = 0, n = coords.length/2; i < n; i++) { 
            a *= coords[n+i] - coords[i];
        }
        return a;
    }

    /**
     * Area of covered rectangle for two sepcified rectangles
     */
    public static double joinArea(RectangleRn a, RectangleRn b) {
        double area = 1.0;
        for (int i = 0, n = a.coords.length/2; i < n; i++) { 
            double min = Math.min(a.coords[i], b.coords[i]);
            double max = Math.max(a.coords[n + i], b.coords[n + i]);
            area *= max - min;
        }
        return area;
    }

    /**
     * Calculate dostance from the specified poin to the rectange
     */
    public double distance(PointRn point) { 
        double d = 0;
        for (int i = 0, n = point.coords.length; i < n; i++) {             
            if (point.coords[i] < coords[i]) { 
                d += (coords[i] - point.coords[i]) * (coords[i] - point.coords[i]);
            } else if (point.coords[i] > coords[n + i]) { 
                d += (coords[n + i] - point.coords[i]) * (coords[n + i] - point.coords[i]);
            }
        }
        return Math.sqrt(d);
    }

    /**
     * Clone rectangle 
     */
    public Object clone() { 
        try { 
            RectangleRn r = (RectangleRn)super.clone();
            r.coords = this.coords;
            return r;
        } catch (CloneNotSupportedException e) { 
            // this shouldn't happen, since we are Cloneable
            throw new InternalError();
        }
    }

    /**
     * Create copy of the rectangle
     */
    public RectangleRn(RectangleRn r) {
        coords = new double[r.coords.length];
        System.arraycopy(r.coords, 0, coords, 0, coords.length);
    }

    /**
     * Construct rectangle with specified coordinates
     */
    public RectangleRn(double[] coords) { 
        this.coords = new double[coords.length];
        System.arraycopy(coords, 0,  this.coords, 0, coords.length);
    }

    /**
     * Construct n-dimensional rectangle using coordinates of two vertexes
     * @param min rectangle vertex with minimal coordinates
     * @param max rectangle vertex with maximal coordinates
     */
    public RectangleRn(PointRn min, PointRn max) { 
        int n = min.coords.length;
        Assert.that(min.coords.length == max.coords.length);
        coords = new double[n*2];
        for (int i = 0; i < n; i++) {             
            Assert.that(min.coords[i] <= max.coords[i]);
            coords[i] = min.coords[i];
            coords[n+i] = max.coords[i];
        }
    }

    /**
     * Join two rectangles. This rectangle is updates to contain cover of this and specified rectangle.
     * @param r rectangle to be joined with this rectangle
     */
    public final void join(RectangleRn r) { 
        for (int i = 0, n = coords.length/2; i < n; i++) {             
            coords[i] = Math.min(coords[i], r.coords[i]);
            coords[i+n] = Math.max(coords[i+n], r.coords[i+n]);
        }
    }
    

    /**
     * Non destructive join of two rectangles. 
     * @param a first joined rectangle
     * @param b second joined rectangle
     * @return rectangle containing cover of these two rectangles
     */
    public static RectangleRn join(RectangleRn a, RectangleRn b) {
        RectangleRn r = new RectangleRn(a);
        r.join(b);
        return r;
    }

    /**
     * Checks if this rectangle intersects with specified rectangle
     */
    public final boolean intersects(RectangleRn r) { 
        for (int i = 0, n = coords.length/2; i < n; i++) {             
            if (coords[i+n] < r.coords[i] || coords[i] > r.coords[i+n]) { 
                return false;
            }
        }
        return true;
    }

    /**
     * Checks if this rectangle contains the specified rectangle
     */
    public final boolean contains(RectangleRn r) { 
        for (int i = 0, n = coords.length/2; i < n; i++) {             
            if (coords[i] > r.coords[i] || coords[i+n] < r.coords[i+n]) { 
                return false;
            }
        }
        return true;
    }

    /**
     * Check if two rectangles are equal
     */
    public boolean equals(Object o) { 
        if (o instanceof RectangleRn) { 
            RectangleRn r = (RectangleRn)o;
            for (int i = 0, n = coords.length; i < n; i++) {             
                if (coords[i] != r.coords[i]) { 
                    return false;
                }
            }
            return true;
        }
        return false;
    }
    
    /**
     * Hash code consists of all rectangle coordinates
     */
    public int hashCode() { 
        long h = 0;
        for (int i = 0, n = coords.length; i < n; i++) {             
            h = (h << 8) | (h >>> 56); /// rotate
            h ^= Double.doubleToLongBits(coords[i]);;
        }
        return (int)(h) ^ (int)(h >>> 32);
    }

    public String toString() { 
        StringBuffer buf = new StringBuffer();
        int n = coords.length/2; 
        buf.append("{(");
        buf.append(coords[0]);
        for (int i = 1; i < n; i++) {             
            buf.append(',');
            buf.append(coords[i]);
        }
        buf.append("),(");
        buf.append(coords[n]);
        for (int i = 1; i < n; i++) {             
            buf.append(',');
            buf.append(coords[i+n]);
        }
        buf.append(")}");
        return buf.toString();
    }
}
    
    








