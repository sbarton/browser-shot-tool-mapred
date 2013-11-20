/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package net.internetmemory.utils;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.io.*;
import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hbase.util.Base64;
import sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl;
import sun.reflect.generics.reflectiveObjects.TypeVariableImpl;


/**
 * A set of utilitary functions, packaged a static methods
 * 
 */
public class Utils {

    public static void outTextTime(String text) {

        System.out.println(text + " -- " + Calendar.getInstance().getTime());

    }

    public static void outArray(Object[] arr) {

        if (arr == null) {
            System.out.println("null");
            return;
        }
        
        System.out.print("[");
        int counter = 0;
        for (Object item : arr) {
            System.out.print(item);
            if (counter + 1 < arr.length) {
                System.out.print(", ");
            }
            counter++;
        }

        System.out.println("]");
    }

    public static void outArray(float[] arr) {

        System.out.print("[");
        int counter = 0;
        float totSum = 0F;
        for (Object item : arr) {
            System.out.print(item);
            if (counter + 1 < arr.length) {
                System.out.print(", ");
            }
            counter++;
            totSum += (Float) item;
        }

        System.out.println("] " + (totSum / arr.length));

    }

    public static void outArray(int[] arr) {

        System.out.print("[");
        int counter = 0;
        float totSum = 0F;
        for (Object item : arr) {
            System.out.print(item);
            if (counter + 1 < arr.length) {
                System.out.print(", ");
            }
            counter++;
            totSum += (Integer) item;
        }

        System.out.println("] " + (totSum / arr.length));

    }

    public static void outArray(double[] arr) {

        System.out.print("[");
        int counter = 0;

        double totSum = 0d;

        for (Object item : arr) {
            System.out.print(item);
            if (counter + 1 < arr.length) {
                System.out.print(", ");
            }
            counter++;
            totSum += (Double) item;
        }

        System.out.println("] " + (totSum / arr.length));
    }

    public static void outArray(byte[] arr) {

        System.out.print("[");
        int counter = 0;

        double totSum = 0d;

        for (Object item : arr) {
            System.out.print(item);
            if (counter + 1 < arr.length) {
                System.out.print(", ");
            }
            counter++;
            totSum += (Byte) item;
        }

        System.out.println("] " + (totSum / arr.length));
    }

    public static void outArray(short[] arr) {

        System.out.print("[");
        int counter = 0;

        double totSum = 0d;

        for (short item : arr) {
            System.out.print(item);
            if (counter + 1 < arr.length) {
                System.out.print(", ");
            }
            counter++;
            totSum += (short) item;
        }

        System.out.println("] " + (totSum / arr.length));
    }

    public static void outMatrix(float[][] matrix) {

        for (int i = 0; i < matrix.length; i++) {
            for (int j = 0; j < matrix[i].length; j++) {
                System.out.print(matrix[i][j]);
                if (j + 1 < matrix[i].length) {
                    System.out.print(", ");
                }
            }
            System.out.println();
        }
    }

    public static float[] parseStringLineToFloats(String line) {

        String[] parts = line.split(" ");
        float[] ret = new float[parts.length];

        int counter = 0;
        for (String part : parts) {
            ret[counter] = Float.parseFloat(part);
            counter++;
        }
        return ret;
    }

    public static Set<Integer> initRandomRows(int maxRows, int maxRowNumber) {
        return initRandomRows(maxRows, maxRowNumber, 100);
    }

    public static Set<Integer> initRandomRows(int maxRows, int maxRowNumber, int seed) {

        int maxVal = maxRowNumber;

        Random r = new Random(100);

        if (seed == 0) {
            r = new Random();
        }

        Set<Integer> maxRowsSet = new TreeSet<Integer>();

        while (maxRowsSet.size() < maxRows) {
            maxRowsSet.add(r.nextInt(maxVal));
        }
        return maxRowsSet;
    }

    public static Set<Long> initRandomRows(int maxRows, long maxRowNumber) {

        long maxVal = maxRowNumber;
        Random r = new Random(100);

        Set<Long> maxRowsSet = new TreeSet<Long>();

        while (maxRowsSet.size() < maxRows) {
            long val = r.nextLong();
            if (val < 0) {
                val *= -1;
            }
            if (val < maxVal) {
                maxRowsSet.add(val);
            }
        }
        return maxRowsSet;
    }
    
    public static Class<?> getGenericTypeParameter(Class<?> cls) {
        return getGenericTypeParameter(cls, 0);
    }
    public static Class<?> getGenericTypeParameter(Class<?> cls, int index) {
    	ParameterizedType tp = null;
    	while(true) {
    		Type t = cls.getGenericSuperclass();
    		if(!(t instanceof ParameterizedType)) {
    			cls = cls.getSuperclass();
    		} else {
	    		tp = (ParameterizedType) t;
	            if(!(tp.getActualTypeArguments()[index] instanceof TypeVariable<?>) 
                            && !(tp.getActualTypeArguments()[index] instanceof TypeVariableImpl<?>)) {
	            	break;
	            } else {
	            	cls = cls.getSuperclass();
	            }
    		}
    	}
    	
        if (tp.getActualTypeArguments()[index] instanceof Class)
            return (Class<?>) tp.getActualTypeArguments()[index];
        else {        
            //TypeVariableImpl
            return ((ParameterizedTypeImpl) tp.getActualTypeArguments()[index]).getClass();
            }
	}
    
    /**
     * Combines the array of strings in one string
     * 
     * @param split array of splits
     * @param glue combine with what     
     * 
     * @return The concatenated string values
     */
    public static String combine(String[] split, String glue) {
        return combine(split, glue, split.length);
    }
    
    /**
     * Combines the array of strings in one string up to a given length
     * 
     * @param split array of splits
     * @param glue combine with what
     * @param length number of splits to split
     * 
     * @return The concatenated string values
     */    
    public static String combine(String[] split, String glue, int length) {
            
        StringBuilder sb = new StringBuilder();
        int truLen = Math.min(split.length, length);
        for (int i=0; i < truLen; i++) {
            sb.append(split[i]);
            if (i+1 < truLen)
                sb.append(glue);
        }
        
        return sb.toString();
    }
    
//    /**
//     * Converts given string to object
//     * 
//     * @param str
//     * @return The object de-serialized from the string
//     */
//    public static Object convertStringToObjectGeneral(String str) {
//
//        byte[] bytes  = Base64.decode(str);
//        Object ret= null;
//
//        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
//
//        try {
//            ObjectInputStream in = new ObjectInputStream(bis);
//
//            ret = in.readObject();
//
//            in.close();
//
//
//        } catch (ClassNotFoundException ex) {
//            Logger.getLogger(Utils.class.getName()).log(Level.SEVERE, null, ex);
//        } catch (IOException ex) {
//            Logger.getLogger(Utils.class.getName()).log(Level.SEVERE, null, ex);
//        }
//        return ret;
//    }
//
//    /**
//     * Materializes given object and converts it to string. !! does not really work now
//     * @param o
//     * @return Object serialization as a string
//     */
//    public static String convertObjectToString(Object o, Class<?> p) {
//
//        ByteArrayOutputStream bos = new ByteArrayOutputStream();
//        try {
//            ObjectOutputStream out = new ObjectOutputStream(bos);
//            
//            System.out.println(o.getClass() + " -- "+p);
//            out.writeObject(p.cast(o));
//            out.close();
//
//
//        } catch (IOException ex) {
//            Logger.getLogger(Utils.class.getName()).log(Level.SEVERE, null, ex);
//        }
//
//        return Base64.encodeBytes(bos.toByteArray());
//    }
    
    /**
     * Returns pid of this JVM
     * @return pid  
     */
    public static int getPID() {
        int pid = -1;
        try {
            String name = ManagementFactory.getRuntimeMXBean().getName();
            String parts [] = name.split("@");
            return Integer.parseInt(parts[0]);
        }
        catch (Exception ex) {
            
        }
        return pid;
    }
    
       /**
     * Transform a number into GBs in text, optionally also with the GB unit
     * 
     * @param number The number to represent
     * @param addUnit Whether we want to add the unit or not
     * 
     * @return Textual representation of the number
     */
    public static String numberInGB(long number, boolean addUnit) {
        float num = number / (float)(1024 * 1024 * 1024);
        num = (float)(Math.round(num*100F)/100F);
        
        return num + (addUnit ? " GB" : "");
    }

    
    /**
     * Parses the generic opts passed via command line and stores them to a static map
     * @param optionValues 
     */
    public static Map<String, String> processGenericOpts(Properties optionProperties) {
        Map<String, String> ret = new HashMap();        
        for (Iterator<Object> it = optionProperties.keySet().iterator(); it.hasNext();) {
            String key = (String) it.next();
            String value = optionProperties.getProperty(key);
            ret.put(key, value);            
        }        
        return ret;
    }
    
    /**
     * Puts the generic opts to the configuration object
     * @param conf 
     */
    public static void setGenericOpts(Configuration conf, Map<String, String> genericOpts) {
       for (Map.Entry<String,String> e : genericOpts.entrySet()) {
           conf.set(e.getKey(), e.getValue());
       } 
    }
    
    /**
     * Output the configuration object to stdout
     * @param conf 
     */
    public static void outConfiguration(Configuration conf) {
        
        for (Iterator i = conf.iterator(); i.hasNext();) {
            Map.Entry<String, String> e = (Map.Entry<String, String>) i.next();            
            System.out.println(e);
        }
        
    }
}
