/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package net.internetmemory.utils;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 *
 * @author barton
 */
public class TautologicFilter implements PathFilter {

    @Override
    public boolean accept(Path path) {
        return true;
    }
    
}
