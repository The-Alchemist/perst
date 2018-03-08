package org.garret.perst;

/**
 * Floating point operations with error
 */
class FP 
{
    static final double EPSILON = 1.0E-06;
    
    static final boolean zero(double x) { 
        return Math.abs(x) <= EPSILON;
    }
    
    static final boolean eq(double x, double y) { 
        return zero(x - y);
    }
    
    static final boolean ne(double x, double y) { 
        return !eq(x, y);
    }
    
    static final boolean lt(double x, double y) { 
        return y - x > EPSILON;
    }
    
    static final boolean le(double x, double y) { 
        return x - y <= EPSILON;
    }
    
    static final boolean gt(double x, double y) { 
        return x - y > EPSILON;
    }
    
    static final boolean ge(double x, double y) { 
        return y - x <= EPSILON;
    }
};


/**
 * Point in 3D
 */
class Point3D 
{
    double x;
    double y;
    double z;

    final Point3D cross(Point3D p) { 
        return new Point3D(y * p.z - z * p.y,
                           z * p.x - x * p.z,
                           x * p.y - y * p.x);
    }
     
    final double distance() {
        return Math.sqrt(x*x + y*y + z*z);
    }
    
    public boolean equals(Object o) { 
        if (o instanceof Point3D)  { 
            Point3D p = (Point3D)o;
            return FP.eq(x, p.x) && FP.eq(y, p.y) && FP.eq(z, p.z);
        }
        return false;
    } 

    final RectangleRn toRectangle() { 
        return new RectangleRn(new double[]{x, y, z, x, y, z});
    }
    
    final Sphere.Point toSpherePoint() { 
        double rho = Math.sqrt(x*x + y*y);
        double lat, lng;
        if (0.0 == rho) {
            if (FP.zero(z)) {
                lat = 0.0;
            } else if (z > 0) {
                lat = Math.PI/2;
            } else {
                lat = -Math.PI/2;
            } 
        } else {
            lat = Math.atan(z / rho);
        }

        lng = Math.atan2(y, x);
        if (FP.zero(lng)){
            lng = 0.0;
        } else { 
            if (lng < 0.0) {
                lng += Math.PI*2 ;
            }
        }
        return new Sphere.Point(lng, lat);
    }       


    void addToRectangle(RectangleRn r) { 
        addToRectangle(r, x, y, z);
    }

    static void addToRectangle(RectangleRn r, double ra, double dec) { 
        double x = Math.cos(ra)*Math.cos(dec);
        double y = Math.sin(ra)*Math.cos(dec);
        double z = Math.sin(dec);
        addToRectangle(r, x, y, z);
    }

    static void addToRectangle(RectangleRn r, double x, double y, double z) { 
        if (x < r.coords[0]) { 
            r.coords[0] = x;
        }
        if (y < r.coords[1]) { 
            r.coords[1] = y;
        }
        if (z < r.coords[2]) { 
            r.coords[2] = z;
        }
        if (x > r.coords[3]) { 
            r.coords[3] = x;
        }
        if (y > r.coords[4]) { 
            r.coords[4] = y;
        }
        if (z > r.coords[5]) { 
            r.coords[5] = z;
        }
    }       
     
    Point3D(double ra, double dec) {
        x = Math.cos(ra)*Math.cos(dec);
        y = Math.sin(ra)*Math.cos(dec);
        z = Math.sin(dec);
    }

    Point3D() {}

    Point3D(Sphere.Point p) {
        this(p.ra, p.dec);
    }

    Point3D(double x, double y, double z) { 
        this.x = x;
        this.y = y;
        this.z = z;
    }
}

/**
 * Euler transformation
 */
class Euler  
{ 
    int phi_a;    // first axis
    int theta_a;  // second axis
    int psi_a;    // third axis
    double phi;   // first rotation angle
    double theta; // second rotation angle
    double psi;   // third rotation angle

    static final int AXIS_X = 1;
    static final int AXIS_Y = 2;
    static final int AXIS_Z = 3;

    void transform(Point3D out, Point3D in)
    {
        int t = 0;
        double a, sa, ca;
        double x1, y1, z1;
        double x2, y2, z2;
        
        x1 = in.x;
        y1 = in.y;
        z1 = in.z;

        for (int i=0; i<3; i++) {
            switch (i) {
            case 0: 
                a = phi; 
                t = phi_a; 
                break;
            case 1: 
                a = theta; 
                t = theta_a; 
                break;
            default: 
                a = psi; 
                t = psi_a; 
                break;
            }
            
            if (FP.zero(a)) {
                continue;
            }
            
            sa = Math.sin(a);
            ca = Math.cos(a);

            switch (t) {                    
            case AXIS_X :
                x2 = x1;
                y2 = ca*y1 - sa*z1;
                z2 = sa*y1 + ca*z1;
                break;
            case AXIS_Y :
                x2 = ca*x1 + sa*z1;
                y2 = y1;
                z2 = ca*z1 - sa*x1;
                break;
            default:
                x2 = ca*x1 - sa*y1;
                y2 = sa*x1 + ca*y1;
                z2 = z1;
                break;               
            }
            x1 = x2;
            y1 = y2;
            z1 = z2;
        }
        out.x = x1;
        out.y = y1;
        out.z = z1;
    }
}


/**
 * Class for conversion equatorial coordinates to Cartesian R3 coordinates
 */
public class Sphere 
{
    /**
     * Common interface for all objects on sphere
     */
    public interface SphereObject extends IValue
    { 
        /**
         * Get wrapping 3D rectangle for this object
         */
        public RectangleRn wrappingRectangle();
        
        /**
         * Check if object contains specified point
         ( @param p sphere point
         */
        public boolean contains(Point p);
    };
            
    /** 
     * Class representing point of equatorial coordinate system (astronomic or terrestrial)
     */
    public static class Point implements SphereObject
    { 
        /** 
         * Right ascension or longitude
         */
        public final double ra;  
        /**
         * Declination or latitude
         */
        public final double dec; 


        public final double latitude() { 
           return dec;
        }

        public final double longitude() { 
            return ra;
        }

        // Fast implementation from Q3C
        public final double distance(Point p) 
        { 
            double x = Math.sin((ra - p.ra) / 2);
            x *= x;
            double y = Math.sin((dec - p.dec) / 2);
            y *= y;
            double z = Math.cos((dec + p.dec) / 2);
            z *= z;
            return 2 * Math.asin(Math.sqrt(x * (z - y) + y));
        }

        public boolean equals(Object o) { 
            if (o instanceof Point)  { 
                Point p = (Point)o;
                return FP.eq(ra, p.ra) && FP.eq(dec, p.dec);
            }
            return false;
        } 

        public RectangleRn wrappingRectangle() { 
            double x = Math.cos(ra)*Math.cos(dec);
            double y = Math.sin(ra)*Math.cos(dec);
            double z = Math.sin(dec);
            return new RectangleRn(new double[]{x, y, z, x, y, z});
        }

        public PointRn toPointRn() { 
            double x = Math.cos(ra)*Math.cos(dec);
            double y = Math.sin(ra)*Math.cos(dec);
            double z = Math.sin(dec);
            return new PointRn(new double[]{x, y, z});
        }

        public boolean contains(Point p) { 
            return equals(p);
        }

        public String toString() { 
            return "(" + ra + "," + dec + ")";
        }

        public Point(double ra, double dec)  {
            this.ra = ra;
            this.dec = dec;
        }
    }

    public static class Box implements SphereObject
    { 
        public final Point sw; // source-west
        public final Point ne; // nord-east

        public boolean equals(Object o) { 
            if (o instanceof Box)  { 
                Box b = (Box)o;
                return sw.equals(b.sw) && ne.equals(b.ne);
            }
            return false;
        }

        public boolean contains(Point p) { 
            return contains(p.ra, p.dec);
        }

        public boolean contains(double ra, double dec) { 
            if ((FP.eq(dec,ne.dec) && FP.eq(dec, Math.PI/2)) || (FP.eq(dec,sw.dec) && FP.eq(dec, -Math.PI/2)))  {
                return true;
            }

            if (FP.lt(dec, sw.dec) || FP.gt(dec, ne.dec)) {
                return false;
            }
            if (FP.gt(sw.ra, ne.ra)) {
                if (FP.gt(ra, sw.ra) || FP.lt(ra, ne.ra)) { 
                    return false;
                } 
            } else {
                if (FP.lt(ra, sw.ra) || FP.gt(ra, ne.ra)) { 
                    return false;
                }
            }
            return true;
        }
           
        public RectangleRn wrappingRectangle() { 
            RectangleRn r = sw.wrappingRectangle();
            double ra, dec;
            Point3D.addToRectangle(r, ne.ra, ne.dec);
            Point3D.addToRectangle(r, sw.ra, ne.dec);
            Point3D.addToRectangle(r, ne.ra, sw.dec);
            
             // latitude closest to equator
            if (FP.ge(ne.dec, 0.0) && FP.le(sw.dec, 0.0)) {
                dec = 0.0;
            } else if (Math.abs(ne.dec) > Math.abs(sw.dec)) {
                dec = sw.dec;
            } else {
                dec = ne.dec;
            }

            for (ra = 0.0; ra < Math.PI*2-0.1; ra += Math.PI/2) {
                if (contains(ra, dec)) { 
                    Point3D.addToRectangle(r, ra, dec);
                }
            }
            return r;
        }

        public Box(Point sw, Point ne) { 
            this.sw = sw;
            this.ne = ne;
        }
    };

    public static class Circle implements SphereObject
    {
        public final Point center;
        public final double radius;
        
        public Circle(Point center, double radius) { 
            this.center = center;
            this.radius = radius;
        }
        
        public String toString() { 
            return "<" + center + "," + radius + ">";
        }

        public boolean equals(Object o) { 
            if (o instanceof Circle)  { 
                Circle c = (Circle)o;
                return center.equals(c.center) && FP.eq(radius, c.radius);
            }
            return false;
        }

        public boolean contains(Point p) { 
            double distance = center.distance(p);
            return FP.le(distance, radius);
        }

        public RectangleRn wrappingRectangle() { 
            Point3D[] v = new Point3D[8];
            Point3D tv = new Point3D();
            Euler euler = new Euler();

            double r = Math.sin(radius);
            double d = Math.cos(radius);

            v[0] = new Point3D(-r, -r, d);
            v[1] = new Point3D(-r, +r, d);
            v[2] = new Point3D(+r, -r, d);
            v[3] = new Point3D(+r, +r, d);
            v[4] = new Point3D(-r, -r, 1.0);
            v[5] = new Point3D(-r, +r, 1.0);
            v[6] = new Point3D(+r, -r, 1.0);
            v[7] = new Point3D(+r, +r, 1.0);
            
            euler.psi_a    = Euler.AXIS_X;
            euler.theta_a  = Euler.AXIS_Z;
            euler.phi_a    = Euler.AXIS_X;
            euler.phi      = Math.PI/2 - center.dec;
            euler.theta    = Math.PI/2 + center.ra;
            euler.psi      = 0.0;

            Point3D min = new Point3D(1.0, 1.0, 1.0);
            Point3D max = new Point3D(-1.0, -1.0, -1.0);

            for (int i=0; i<8; i++) {
                euler.transform(tv, v[i]);
                if (tv.x < -1.0) {
                    min.x = -1.0;
                } else if (tv.x > 1.0) {
                    max.x = 1.0;
                } else { 
                    if (tv.x < min.x) { 
                        min.x = tv.x;
                    }
                    if (tv.x > max.x) { 
                        max.x = tv.x;
                    }
                }
      
                if (tv.y < -1.0) {
                    min.y = -1.0;
                } else if ( tv.y > 1.0 ) {
                    max.y = 1.0;
                } else {
                    if (tv.y < min.y) { 
                        min.y = tv.y;
                    }
                    if (tv.y > max.y) { 
                        max.y = tv.y;
                    }
                }
                if (tv.z < -1.0) {
                    min.z = -1.0;
                }  else if (tv.z > 1.0) {
                    max.z = 1.0;
                } else { 
                    if (tv.z < min.z) { 
                        min.z = tv.z;
                    }
                    if (tv.z > max.z) { 
                        max.z = tv.z;
                    }
                } 
            }
            return new RectangleRn(new double[]{min.x, min.y, min.z, max.x, max.y, max.z});
        }
    }
     
    /**
     * A spherical ellipse is represented using two radii and
     * a Euler transformation ( ZXZ-axis ). The "untransformed"
     * ellipse is located on equator at position (0,0). The 
     * large radius is along equator.    
     */
    public static class Ellipse implements SphereObject
    {
        /** 
         * The large radius of an ellipse in radians
         */
        public final double rad0; 
        /**
         * The small radius of an ellipse in radians
         */
        public final double rad1; 
        /**
         * The first  rotation angle around z axis
         */
        public final double phi;  
        /**
         * The second rotation angle around x axis
         */
        public final double theta;
        /**
         * The last   rotation angle around z axis
         */
        public final double psi;  
        
        public Ellipse(double rad0, double rad1, double phi, double theta, double psi) { 
            this.rad0 = rad0;
            this.rad1 = rad1;
            this.phi = phi;
            this.theta = theta;
            this.psi = psi;
        }            
        
        public Point center() { 
            return new Point(psi, -theta);
        }

        public boolean contains(Point p) { 
            // too complex implementation
            throw new UnsupportedOperationException();
        }            

        public boolean equals(Object o) { 
            if (o instanceof Ellipse)  { 
                Ellipse e = (Ellipse)o;
                return FP.eq(rad0, e.rad0) && FP.eq(rad1, e.rad1) && FP.eq(phi, e.phi) && FP.eq(theta, e.theta) && FP.eq(psi, e.psi);
            }
            return false;
        }

        public RectangleRn wrappingRectangle() { 
            Point3D[] v = new Point3D[8];
            Point3D tv = new Point3D();
            Euler euler = new Euler();

            double r0 = Math.sin(rad0);
            double r1 = Math.sin(rad1);
            double d = Math.cos(rad1);

            v[0] = new Point3D(d, -r0, -r1);
            v[1] = new Point3D(d, +r0, -r1);
            v[2] = new Point3D(d, -r0, r1);
            v[3] = new Point3D(d, +r0, r1);
            v[4] = new Point3D(1.0, -r0, -r1);
            v[5] = new Point3D(1.0, +r0, -r1);
            v[6] = new Point3D(1.0, -r0, r1);
            v[7] = new Point3D(1.0, +r0, r1);
            
            euler.psi_a    = Euler.AXIS_Z ;
            euler.theta_a  = Euler.AXIS_Y;
            euler.phi_a    = Euler.AXIS_X;
            euler.phi      = phi;
            euler.theta    = theta;
            euler.psi      = psi;

            Point3D min = new Point3D(1.0, 1.0, 1.0);
            Point3D max = new Point3D(-1.0, -1.0, -1.0);

            for (int i=0; i<8; i++) {
                euler.transform(tv, v[i]);
                if (tv.x < -1.0) {
                    min.x = -1.0;
                } else if (tv.x > 1.0) {
                    max.x = 1.0;
                } else { 
                    if (tv.x < min.x) { 
                        min.x = tv.x;
                    }
                    if (tv.x > max.x) { 
                        max.x = tv.x;
                    }
                }
      
                if (tv.y < -1.0) {
                    min.y = -1.0;
                } else if ( tv.y > 1.0 ) {
                    max.y = 1.0;
                } else {
                    if (tv.y < min.y) { 
                        min.y = tv.y;
                    }
                    if (tv.y > max.y) { 
                        max.y = tv.y;
                    }
                }
                if (tv.z < -1.0) {
                    min.z = -1.0;
                }  else if (tv.z > 1.0) {
                    max.z = 1.0;
                } else { 
                    if (tv.z < min.z) { 
                        min.z = tv.z;
                    }
                    if (tv.z > max.z) { 
                        max.z = tv.z;
                    }
                } 
            }
            return new RectangleRn(new double[]{min.x, min.y, min.z, max.x, max.y, max.z});
        }
    }
            
    public static class Line implements SphereObject
    {
        /**
         * The first  rotation angle around z axis
         */
        public final double phi;  
        /**
         * The second rotation angle around x axis
         */
        public final double theta;
        /**
         * The last   rotation angle around z axis
         */
        public final double psi;  
        /** 
         * The length of the line in radians
         */
        public final double length; 
        
        public boolean equals(Object o) { 
            if (o instanceof Line) { 
                Line l = (Line)o;
                return FP.eq(phi, l.phi) && FP.eq(theta, l.theta) && FP.eq(psi, l.psi) && FP.eq(length, l.length);
            }
            return false;
        }

        public boolean contains(Point p) { 
            Euler euler = new Euler();
            Point3D spt = new Point3D();
            euler.phi     = -psi;
            euler.theta   = -theta;
            euler.psi     = -phi;
            euler.psi_a   = Euler.AXIS_Z;
            euler.theta_a = Euler.AXIS_X;
            euler.phi_a   = Euler.AXIS_Z;
            euler.transform(spt, new Point3D(p));
            Point sp = spt.toSpherePoint();
            return FP.zero(sp.dec) && FP.ge(sp.ra, 0.0) && FP.le(sp.ra, length);
        }                      
            
        public static Line meridian(double ra) { 
            return new Line(-Math.PI/2, Math.PI/2, ra < 0.0 ? Math.PI*2 + ra : ra, Math.PI);
        }

        public Line(double phi, double theta, double psi, double length) {
            this.phi = phi;
            this.theta = theta;
            this.psi = psi;
            this.length = length;
        }            
        
        public Line(Point beg, Point end) { 
            double l = beg.distance(end);
            if (FP.eq(l, Math.PI)) {
                Assert.that(FP.eq(beg.ra, end.ra));
                phi = -Math.PI/2;
                theta = Math.PI/2;
                psi = beg.ra < 0.0 ? Math.PI*2 + beg.ra : beg.ra;
                length = Math.PI;
                return;
            }
            if (beg.equals(end)) { 
                phi    = Math.PI/2;
                theta  = beg.dec;
                psi    = beg.ra - Math.PI/2;
                length = 0.0;
            } else { 
                Point3D beg3d = new Point3D(beg);
                Point3D end3d = new Point3D(end);
                Point3D tp = new Point3D();
                Point spt = beg3d.cross(end3d).toSpherePoint();
                Euler euler = new Euler();
                euler.phi     = - spt.ra - Math.PI/2;
                euler.theta   =   spt.dec - Math.PI/2;
                euler.psi     =   0.0 ;
                euler.psi_a   = Euler.AXIS_Z;
                euler.theta_a = Euler.AXIS_X;
                euler.phi_a   = Euler.AXIS_Z;
                euler.transform(tp, beg3d);
                spt = tp.toSpherePoint();

                // invert
                phi = spt.ra;
                theta = -euler.theta;
                psi = -euler.phi;
                length = l;
            }
        }

        public RectangleRn wrappingRectangle() { 
           Euler euler = new Euler();
           euler.phi      = phi;
           euler.theta    = theta;
           euler.psi      = psi;
           euler.psi_a    = Euler.AXIS_Z ;
           euler.theta_a  = Euler.AXIS_X;
           euler.phi_a    = Euler.AXIS_Z;
           
           if (FP.zero(length)) {
               Point3D beg3d = new Point3D();
               Point3D end3d = new Point3D();
               euler.transform(beg3d, new Point3D(0.0, 0.0));
               euler.transform(end3d, new Point3D(length, 0.0));
               RectangleRn r = beg3d.toRectangle();
               end3d.addToRectangle(r);
               return r;
           } else { 
               double l, ls, lc;
               Point3D[] v = new Point3D[4];
               Point3D tv = new Point3D();
               l  = length / 2.0; 
               ls = Math.sin(l);
               lc = Math.cos(l);
               euler.phi += l;
               
               v[0] = new Point3D(lc,  lc<0 ? -1.0 : -ls, 0.0);
               v[1] = new Point3D(1.0, lc<0 ? -1.0 : -ls, 0.0);
               v[2] = new Point3D(lc,  lc<0 ? +1.0 : +ls, 0.0);
               v[3] = new Point3D(1.0, lc<0 ? +1.0 : +ls, 0.0) ;

               Point3D min = new Point3D(1.0, 1.0, 1.0);
               Point3D max = new Point3D(-1.0, -1.0, -1.0);

               for (int i=0; i<4; i++) {
                   euler.transform(tv, v[i]);
                   if (tv.x < -1.0) {
                       min.x = -1.0;
                   } else if (tv.x > 1.0) {
                       max.x = 1.0;
                   } else { 
                       if (tv.x < min.x) { 
                           min.x = tv.x;
                       }
                       if (tv.x > max.x) { 
                           max.x = tv.x;
                       }
                   }
                   
                   if (tv.y < -1.0) {
                       min.y = -1.0;
                   } else if ( tv.y > 1.0 ) {
                       max.y = 1.0;
                   } else {
                       if (tv.y < min.y) { 
                           min.y = tv.y;
                       }
                       if (tv.y > max.y) { 
                           max.y = tv.y;
                       }
                   }
                   if (tv.z < -1.0) {
                       min.z = -1.0;
                   } else if (tv.z > 1.0) {
                       max.z = 1.0;
                   } else { 
                       if (tv.z < min.z) { 
                           min.z = tv.z;
                       }
                       if (tv.z > max.z) { 
                           max.z = tv.z;
                       }
                   } 
               }  
               return new RectangleRn(new double[]{min.x, min.y, min.z, max.x, max.y, max.z});
           }
       }
    }

    public static class Polygon implements SphereObject 
    { 
        public final Point[] points;
        
        public Polygon(Point[] points) { 
            this.points = points;
        }

        public boolean contains(Point p) { 
            throw new UnsupportedOperationException();
        }            

        public RectangleRn wrappingRectangle() { 
            RectangleRn wr = null;
            for (int i=0; i < points.length; i++) {
                Line line = new Line(points[i], points[(i+1) % points.length]);
                RectangleRn r = line.wrappingRectangle();
                if (wr == null) { 
                    wr = r;
                } else { 
                    wr.join(r);
                }
            }
            return wr;
        }
    }
}
