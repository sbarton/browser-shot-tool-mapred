/*
 */

package net.internetmemory.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Progress tracking utility, especially when several observations of one event needs to be recorded.
 * It makes simple recording time for pieces of code. Each tracker has unique name and sequence of recorded
 * values, the sequence numbers are assigned automatically.
 */
public class TrackerNumber {
    private Map<String, Object> startVals = new HashMap<String, Object>();
    private Map<String, Object> endVals = new HashMap<String, Object>();

    private Map<String, NormMap> progress = new HashMap<String, NormMap>();
	private TaskAttemptContext taskContext;        

    /**
     * Starts a tracker with a specified value
     * @param name Name of the tracker
     * @param value initial value of the tracker event
     */
    public void startTracker(String name, Object value) {
        startVals.put(name, value);
        if (progress.get(name) == null)
            progress.put(name, new NormMap());
    }

    /**
     * Ends a tracker with the specified name
     * @param name Name of the tracker
     * @param value final value of the tracker event
     */
    public void endTracker (String name, Object value) {
        if (startVals.keySet().contains(name)) {
            endVals.put(name, value);
            
            double startVal = ((Number)startVals.get(name)).doubleValue();
            double endVal = ((Number)endVals.get(name)).doubleValue();

            NormMap nm = progress.get(name);
            nm.putItemLast((float)(endVal - startVal));
            //nm.putItemLast((float)((endVal - startVal)/1000000F));
            progress.put(name, nm);

        }
        else {
            
            //too harsh
            //throw new UnsupportedOperationException("Name "+name+" not in startVals");
            System.out.println("TRACKER: Name "+name+" not in startVals");
        }
    }

    /**
     * Starts a tracker event for a specified name with a value being current time in milliseconds.
     * @param name 
     */
     
    public void startTrackerMilis(String name) {
        startTracker(name, System.currentTimeMillis());
        //System.out.println((double)(System.nanoTime()/1000000d));
        //startTracker(name, System.nanoTime());
        //startTracker(name, (double)(System.nanoTime()/1000000d));
    }
    
    /**
     * Starts a tracker event for a specified name with a value being current time in nanoseconds.
     * @param name 
     */
    public void startTrackerNanos(String name) {
        startTracker(name, System.nanoTime());
    }

    /**
     * Ends a tracker event for a specified name with a value being current time in milliseconds.
     * @param name 
     */
    public void endTrackerMilis(String name) {
        endTracker(name, System.currentTimeMillis());
        //endTracker(name, (System.nanoTime()/1000000F));
        //endTracker(name, System.nanoTime());
        
    }
    
    /**
     * Ends a tracker event for a specified name with a value being current time in nanoseconds.
     * @param name 
     */
    public void endTrackerNanos(String name) {
        endTracker(name, System.nanoTime());
    }

    /**
     * Add a value to the sequence of tracker events. It is identical to 
     * starting a tracker with 0 and ending with value. 
     * @param name
     * @param value 
     */
    public synchronized void  addSingleValue(String name, Object value) {
        startTracker(name, 0);
        endTracker(name, value);
    }

    /**
     * Returns a list of all values for a given tracker
     * 
     * @param name
     * @return The list of values for the given name
     */
    public List<Float> getListOfValues(String name) {
        if (progress.get(name) == null) {
            System.out.println("Tracker "+name+" not found.");
            return new ArrayList();
        }
        return new ArrayList<Float>(progress.get(name).values());
    }

    /**
     * Clear tracker and its progress 
     * @param name 
     */
    public void clearTracker(String name) {
        startVals.remove(name);
        endVals.remove(name);
        progress.remove(name);                    
    }
    
    /**
     * Clear all trackers
     */
    public void clearTrackers() {
        startVals.clear();
        endVals.clear();
        progress.clear();        
    }
    
    /**
     * Output all current values for all trackers.
     */
    public void outTrackers() {
        for (String name : startVals.keySet()) {
            outTracker(name);
        }
    }

    /**
     * Get last tracker value, be sure to call after end tracker is called
     * @param name of the tracker 
     * @return last tracked value
     */
    
    public Number getLastTrackerValue(String name) {
        double startVal = ((Number)startVals.get(name)).doubleValue();
        double endVal = ((Number)endVals.get(name)).doubleValue();
        return (endVal - startVal);
    }
    
    /**
     * Output last value for a given tracker
     * @param name 
     */
     
    public void outTracker(String name) {
        if (startVals.get(name) == null || endVals.get(name) == null) {
            System.out.println(name +" - not ended.");
        }
        else {
            double startVal = ((Number)startVals.get(name)).doubleValue();
            double endVal = ((Number)endVals.get(name)).doubleValue();


            System.out.println(name+": "+(endVal - startVal));
        }
    }
    
    public Set<String> getTrackers() {
    	return progress.keySet();
    }

    /**
     * Return a string containing a list of all tracked trackers and their last 
     * values, all on one line
     * @return all trackers and their last value on one line as String
     */
    public String getTrackersFlat() {
        String ret = "";
        for (String name : startVals.keySet()) {
            ret += getTrackerFlat(name)+ ", ";
        }
        return ret;
    }
    
    public NormMap getTracker(String name) {
    	return progress.get(name);
    }

    /**
     * Returns a string of a value for a given tracker for output purposes
     * @param name
     * @return last value of a tracker as a string preceded with ":"
     */
    private String getTrackerFlat(String name) {

        double startVal = ((Number)startVals.get(name)).doubleValue();
        double endVal = ((Number)endVals.get(name)).doubleValue();        
        return name+": "+(endVal - startVal);
    }

    /**
     * Output all trackers with all the recorded values
     */
    public void outProgressAll() {
        outProgressAll(false);
    }

    /**
     * Output all trackers with or without all recorded values
     * @param withoutProgress 
     */
    public void outProgressAll(boolean withoutProgress) {
        TreeSet<String> orderedNames = new TreeSet<String>(startVals.keySet());
        for (String name : orderedNames) {
            outProgress(name, withoutProgress);
        }
    }

    /**
     * Output all trackers on one line with average value
     */
    public void outProgressAllFlat() {
        outProgressAllFlat("");
    }

    /**
     * Output all trackers on one line with average value
     * @param prefix A string that appears as a line prefix
     */
     
    public void outProgressAllFlat(String prefix) {        
        System.out.println(progressAllFlat(prefix));
    }

    /**
     * Get all trackers and their average values as a string on one line
     * @param prefix A string that appears as a line prefix
     * @return 
     */
    private String progressAllFlat(String prefix) {
        String ret = prefix;

        TreeSet<String> orderedNames = new TreeSet<String>(startVals.keySet());

        for (String name : orderedNames) {
            ret += progressFlat(name);
        }
        return ret;
    }

    /**
     * Returns a string for a given tracker followed by ":" and its average value
     * @param name
     * @return String containing the name of th tracker and its average value
     */
    private String progressFlat(String name) {
        String ret = name+": "+progress.get(name).avg()+", ";
        return ret;
    }

    /**
     * Returns a string for a given tracker followed by ":" and a value on a 
     * given position
     * @param name
     * @param positionKey
     * @return String with tracker name and value on a given position
     */
    private String progressFlatForKey(String name, Object positionKey) {
        String ret = name+": "+progress.get(name).get(positionKey)+", ";
        return ret;
    }

    /**
     * Gets a string containing on i-th line all trackers with respective i-th value, 
     * works fine when all of the trackers have same amount of tracked values
     * 
     * @return The string representation of the tracker
     */    
    public String allFlat2String() {
        String ret = "";

        TreeSet<String> orderedNames = new TreeSet<String>(startVals.keySet());

        for (Object positionKey : progress.values().iterator().next().keySet()) {

            //ret += positionKey +"\t";
            for (String name : orderedNames) {
                ret +=  progressFlatForKey(name, positionKey);
            }
            ret +="\n";
        }
        return ret;
    }

    /**
     * Get the tracker progress values represented as a NormMap
     * @param name 
     * @return NormMap containing progress values for a given tracker
     */
    public NormMap getNormMap (String name) {
        return progress.get(name);
    }

    /**
     * Outputs progress for a given tracker (with or without all recorded values).
     * If withoutProgress, only average and sum of all values is present
     * @param name
     * @param withoutProgress 
     */
    public void outProgress(String name, boolean withoutProgress) {
        if (withoutProgress)
            System.out.println(name+":"+ " avg: "+progress.get(name).avg() + " median: "+progress.get(name).median()+ " sum: "+progress.get(name).sum()+ " size : "+progress.get(name).size());
        else
            System.out.println(name+":  avg: "+progress.get(name).avg() + " median: "+progress.get(name).median() + " sum: "+progress.get(name).sum()+ " size : "+progress.get(name).size()+ ", "+ progress.get(name).toString() );
    }

    public static void main(String[] args) {
        TrackerNumber t = new TrackerNumber();

        //t.startTracker("muj 1", System.currentTimeMillis());
        t.startTracker("muj 2", (long)11);
        t.startTracker("muj 3", 15.5);


System.out.println((Number)System.currentTimeMillis());
        for (int i = 0; i < 1000000; i++) {
            
        }
System.out.println((Number)System.currentTimeMillis());
        t.endTracker("muj 1", System.currentTimeMillis());
        t.endTracker("muj 2", (long)15);
        t.endTracker("muj 3", 20F);

        t.outTrackers();

        //t.startTracker("muj 1", 55L);
        //t.endTracker("muj 1", 65L);

        t.outTrackers();

        t.outProgressAll();
        t.outProgressAllFlat("!");

        //System.out.println(t.allFlat2String());

        System.out.println(t.getListOfValues("muj 1"));

    }

	public void reportProgress() {
		if(this.taskContext != null) {
			this.taskContext.progress();
		}
	}

	public void setTaskContext(TaskAttemptContext context) {
		this.taskContext = context;
	}

}
