/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package net.internetmemory.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author xbarton
 */
public class NormMap extends TreeMap<Object,Float>{

    protected long counter = 0;
    protected float roundFactor = 100;

    protected float limit = 0;


    public NormMap() {
        super();

    }

    public NormMap(float rf) {
        roundFactor = rf;
    }

    public NormMap(float rf, float limit) {
        super();
        roundFactor = rf;
        this.limit = limit;
    }

    public long getCounter() {
        return counter;
    }

    public void putItem(Object item) {
        putItem(item, 1F);
    }

    public void putItems(List<Float> list) {
        for (Float item : list) {
            putItem(item);
        }
    }

    public void putItems(NormMap nm) {

        for (Object key : nm.keySet()) {
            putItem(key, nm.get(key));
        }
        
    }

    public void putItemLast(Float number) {
        putItem(new Float(values().size()), number);
    }

    public void putItem(Object item, Float number) {

        if (item instanceof Float && roundFactor > 0) {
            item = Math.round(((Float) item) * roundFactor )/ roundFactor;
        }

        if (limit== 0 || (item instanceof Float && (Float)item <= limit)) {

            Float retrieved = this.get(item);

            if (retrieved == null) {
                retrieved = number;
    //            put(item, retrieved);
            }
            else {
                retrieved +=number;
            }
            put(item, retrieved);

            counter += number.intValue();
        }

    }

    
    public void writeFileCsv (File f) {
        BufferedWriter output = null;
        try {
            output = new BufferedWriter(new FileWriter(f));


            for (Object item : keySet()) {
                Float value = get(item);
                //output.write("\""+item+"\", \""+value+"\"");
                output.write(item+", "+value);
                output.newLine();
            }


        } catch (IOException ex) {
            Logger.getLogger(NormMap.class.getName()).log(Level.SEVERE, null, ex);
        }
        finally {
            try {
                output.close();
            } catch (IOException ex) {
                Logger.getLogger(NormMap.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

    }

    public void writeFileTxt (String fName) {
        writeFileTxt (new File(fName));
    }
    
    public void writeFileTxt (File f) {
        BufferedWriter output = null;
        try {
            output = new BufferedWriter(new FileWriter(f));


            for (Object item : keySet()) {
                Float value = get(item);
                
                output.write(item+"\t"+value);
                output.newLine();
            }


        } catch (IOException ex) {
            Logger.getLogger(NormMap.class.getName()).log(Level.SEVERE, null, ex);
        }
        finally {
            try {
                output.close();
            } catch (IOException ex) {
                Logger.getLogger(NormMap.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

    }

    public void outLLCode() {
        int counter=0;
        System.out.println("int offset=;");
        for (Object key : keySet()) {
            System.out.println("a[offset+"+counter+"]="+key + "; y[offset+"+counter+"]="+ get(key)+";");
            counter++;
        }
    }

    public NormMap createSlots(int slots) {
        NormMap tmp = new NormMap(roundFactor);

        try {
             //Float smallest = (Float) firstKey();
            Float smallest =  Float.parseFloat(firstKey().toString());
             Float greatest = Float.parseFloat(lastKey().toString());

             float slot = (greatest - smallest) / (float)slots;


             float leftLimit = smallest;
             float rightLimit = smallest + slot;



             for (Object key : this.keySet()) {
                 Float val = get(key);
                 float keyFloat = Float.parseFloat(key.toString());

                 if (keyFloat > rightLimit) {
                     leftLimit = rightLimit;
                     rightLimit = rightLimit+slot;

                 }
                 tmp.putItem((leftLimit + (slot / 2)), val);
             }
        }
        catch (NoSuchElementException e) {

        }
         return tmp;
         
    }

    public void normalize() {
        for (Object item : keySet()) {
            Float value = get(item);

            value /= counter;

            this.put(item, value);
        }
    }

    public float avg() {
        float avg = 0;
        for (float mem : values()) {
            avg += mem;
        }
        return avg / (float)(values().size());
    }

    public float count() {
        return values().size();
    }

    public float sum() {
        float sum = 0;
        for (float mem : values()) {
            sum += mem;
        }
        return sum;
    }
    
    public float median() {
    	Float median = null;
        if (keySet().size() > 0 ) {
            median = get((float)Math.floor(keySet().size()/2));
        }
        if(median == null) {
        	return Float.NaN;
        } else {
        	return median;
        }
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
                
        str.append(size()).append("{");
                
        int ctr = 0;
        for (Object item : keySet()) {
            Float value = get(item);

            str.append (item).append("=").append(value);
            if (ctr+1 < size()) {
                str.append(", ");
            }
            ctr++;
        }
        str.append("}");

        return str.toString();

    }
    
    public static void main(String[] args) {
        NormMap nm = new NormMap();
        /*
        for (int i = 0; i < 10; i++) {
            nm.putItemLast((float)i);
        }*/
        
        System.out.println(nm.median());
        
    }

}
