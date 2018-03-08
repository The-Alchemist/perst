package org.garret.perst.assoc;

/**
 * Helper class for manipulations with arrays
 */
public class Array
{
    public static int[] remove(int[] arr, int i) { 
        return remove(arr, i, 1);
    }

    public static int[] remove(int[] arr, int i, int count) { 
        int n = arr.length;
        int[] newArr = new int[n-count];
        System.arraycopy(arr, 0, newArr, 0, i);
        System.arraycopy(arr, i+count, newArr, i, n-i-count);
        return newArr;
    }

    public static String[] remove(String[] arr, int i) { 
        return remove(arr, i, 1);
    }

    public static String[] remove(String[] arr, int i, int count) { 
        int n = arr.length;
        String[] newArr = new String[n-count];
        System.arraycopy(arr, 0, newArr, 0, i);
        System.arraycopy(arr, i+count, newArr, i, n-i-count);
        return newArr;
    }

    public static double[] remove(double[] arr, int i) { 
        return remove(arr, i, 1);
    }

    public static double[] remove(double[] arr, int i, int count) { 
        int n = arr.length;
        double[] newArr = new double[n-count];
        System.arraycopy(arr, 0, newArr, 0, i);
        System.arraycopy(arr, i+count, newArr, i, n-i-count);
        return newArr;
    }

    public static String[] insert(String[] arr, int i, String value) { 
        int n = arr.length;
        String[] newArr = new String[n+1];
        System.arraycopy(arr, 0, newArr, 0, i);
        newArr[i] = value;
        System.arraycopy(arr, i, newArr, i+1, n-i);
        return newArr;
    }

    public static double[] insert(double[] arr, int i, double value) { 
        int n = arr.length;
        double[] newArr = new double[n+1];
        System.arraycopy(arr, 0, newArr, 0, i);
        newArr[i] = value;
        System.arraycopy(arr, i, newArr, i+1, n-i);
        return newArr;
    }

    public static int[] insert(int[] arr, int i, int value) { 
        return insert(arr, i, value, 1);
    }

    public static int[] insert(int[] arr, int i, int value, int count) { 
        int n = arr.length;
        int[] newArr = new int[n+count];
        System.arraycopy(arr, 0, newArr, 0, i);
        for (int j = 0; j < count; j++) { 
            newArr[i+j] = value;
        }
        System.arraycopy(arr, i, newArr, i+count, n-i);
        return newArr;
    }

    public static String[] insert(String[] arr, int i, String[] values) { 
        int n = arr.length;
        String[] newArr = new String[n+values.length];
        System.arraycopy(arr, 0, newArr, 0, i);
        System.arraycopy(values, 0, newArr, i, values.length);
        System.arraycopy(arr, i, newArr, i+values.length, n-i);
        return newArr;
    }

    public static double[] insert(double[] arr, int i, double[] values) { 
        int n = arr.length;
        double[] newArr = new double[n+values.length];
        System.arraycopy(arr, 0, newArr, 0, i);
        System.arraycopy(values, 0, newArr, i, values.length);
        System.arraycopy(arr, i, newArr, i+values.length, n-i);
        return newArr;
    }

    public static int[] truncate(int[] arr, int size) { 
        int[] newArr = new int[size];
        System.arraycopy(arr, 0, newArr, 0, size);
        return newArr;
    }
}
