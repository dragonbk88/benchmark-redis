package com.ird;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

public class ReadCacheFile {
	private static ExecutorService executors;
	private static Logger logger = Logger.getLogger(ReadCacheFile.class);
	
	private static void list(File file) {
	    if (file.isFile()) {
	    	executors.execute(new FileHandler(file));
	    } else {
	    	File[] children = file.listFiles();
		    for (File child : children) {
		        list(child);
		    }
	    }
	}
	
	public static void main(String[] args) {
		logger.info("<<< Starting push cache key into redis");
		logger.info("Start time: " + new Date());
		InputStream input = null;
		try {
			long start = System.currentTimeMillis();
			Properties prop = new Properties();
			input = new FileInputStream("config/purgefolder.conf");
			prop.load(input);
			int NUM_CPUS = Integer.parseInt(prop.getProperty(Constants.NUM_CPU));
			String folderPurge = prop.getProperty(Constants.PURGE_FOLDER);
			File folder = new File(folderPurge);

			executors = Executors.newFixedThreadPool(NUM_CPUS);
			list(folder);
			
			executors.shutdown();

            while(!executors.isTerminated());
            
            logger.info("End time: " + new Date());
            logger.info("Total time : "+(System.currentTimeMillis()-start)/1000.0 + " s ");
            logger.info(">>> End push cache key to redis");
			
		} catch (Exception e) {
			logger.error("Read cache file error: " + e.getMessage(), e);
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					logger.error("close file error: " + e.getMessage(), e);
				}
			}
		}
	}
}
