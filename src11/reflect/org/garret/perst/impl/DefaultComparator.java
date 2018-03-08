package org.garret.perst.impl;

import org.garret.perst.*;
import java.util.Date;

public class DefaultComparator implements Comparator 
{ 
    public int compare(Object o1, Object o2) 
    {
        if (o1 == o2) { 
            return 0;
        }  if (o1 == null) { 
            return -1;
        } else if (o2 == null) { 
            return 1;
        } else if (Number.isRealNumber(o1) || Number.isRealNumber(o2)) { 
            double v1 = Number.doubleValue(o1);
            double v2 = Number.doubleValue(o2);
            return v1 < v2 ? -1 : v1 == v2 ? 0 : 1;
        } else if (Number.isNumber(o1) || Number.isNumber(o2)) { 
            long v1 = Number.longValue(o1);
            long v2 = Number.longValue(o2);
            return v1 < v2 ? -1 : v1 == v2 ? 0 : 1;
        } else if (o1 instanceof String) { 
            return ((String)o1).compareTo((String)o2);
        } else if (o1 instanceof Date) { 
            long v1 = ((Date)o1).getTime();
            long v2 = ((Date)o2).getTime();
            return v1 < v2 ? -1 : v1 == v2 ? 0 : 1;
        } else { 
            return ((org.garret.perst.Comparable)o1).compareTo(o2);
        }
    }

    public boolean equals(Object obj) { 
        return obj == this;
    }

    public static DefaultComparator instance = new DefaultComparator();
}
