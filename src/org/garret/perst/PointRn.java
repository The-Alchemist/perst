package org.garret.perst;

/**
 * R-n point class. This class is used in spatial index.
 */
public class PointRn implements IValue, Cloneable 
{
    double[] coords;

    /**
     * Get value for i-th coordinate
     */
    public final double getCoord(int i) { 
        return coords[i];
    }

    /**
     * Constructor
     */
    public PointRn(double[] coords) { 
        this.coords = new double[coords.length];
        System.arraycopy(coords, 0, this.coords, 0, coords.length);
    }

    public String toString() {
        StringBuffer buf = new StringBuffer();
        buf.append('(');
        buf.append(coords[0]);
        for (int i = 1; i < coords.length; i++) { 
            buf.append(',');
            buf.append(coords[i]);
        }
        buf.append(')');
        return buf.toString();
    }            
}