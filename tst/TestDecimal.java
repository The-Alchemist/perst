import  org.garret.perst.*;      

public class TestDecimal { 
    static final int INT_DIGITS = 5;
    static final int FRAC_DIGITS = 2;

    public static void main(String[] args) { 
        Decimal d1 = new Decimal(12345, INT_DIGITS, FRAC_DIGITS);        
        Decimal d2 = new Decimal(12.34, INT_DIGITS, FRAC_DIGITS);
        Decimal d3 = new Decimal("1.23", INT_DIGITS, FRAC_DIGITS);        
        Decimal d4 = new Decimal("00001.00");
        Decimal d5 = new Decimal(-12345, INT_DIGITS, FRAC_DIGITS);        
        Decimal d6 = new Decimal(-12.34, INT_DIGITS, FRAC_DIGITS);
        Decimal d7 = new Decimal("    -1.23", INT_DIGITS, FRAC_DIGITS);        
        Decimal d8 = new Decimal("-00001.00");

        Assert.that(d1.add(d2).equals(new Decimal(13579, INT_DIGITS, FRAC_DIGITS)));
        Assert.that(d1.sub(d6).equals(new Decimal(13579, INT_DIGITS, FRAC_DIGITS)));
        Assert.that(d3.floor() == 1);
        Assert.that(d7.floor() == -2);
        Assert.that(d4.floor() == 1);
        Assert.that(d8.floor() == -1);
        Assert.that(d3.ceil() == 2);
        Assert.that(d7.ceil() == -1);
        Assert.that(d4.ceil() == 1);
        Assert.that(d8.ceil() == -1);
        Assert.that(d3.round() == 1);
        Assert.that(d7.round() == -1);
        Assert.that(d4.round() == 1);
        Assert.that(d8.round() == -1);
        Assert.that(d1.neg().equals(d5));
        Assert.that(d4.neg().equals(d8));
        Assert.that(d1.compareTo(d2) > 0);
        Assert.that(d3.compareTo(d2) < 0);
        Assert.that(d5.compareTo(d6) < 0);
        Assert.that(d7.compareTo(d6) > 0);
        Assert.that(d4.compareTo(d8.abs()) == 0);
        Assert.that(d1.toString(' ').equals("   123.45"));
        Assert.that(d5.toString(' ').equals("  -123.45"));
        Assert.that(d1.toString().equals("123.45"));
        Assert.that(d5.toString().equals("-123.45"));
        Assert.that(d1.toLexicographicString().equals("100123.45"));
        Assert.that(d5.toLexicographicString().equals("099876.55"));
    }
}
