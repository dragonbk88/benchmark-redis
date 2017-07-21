package com.ird;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class FileHandler implements Runnable {
	private static Logger logger = Logger.getLogger(FileHandler.class);
	private File file;
	private static int MAXLINE = 2; // max number of lines to read
	private static JedisPool jedisPool;
	private static String purgeFolder;
	private static String redisType;

	static {
		logger.info("static init");
		InputStream input = null;
		Properties prop = new Properties();
		try {
			input = new FileInputStream("config/purgefolder.conf");
			prop.load(input);
			String host = prop.getProperty(Constants.REDIS_HOST);
			int port = Integer.parseInt(prop.getProperty(Constants.REDIS_PORT));
			String pass = prop.getProperty(Constants.REDIS_PASS);
			redisType = prop.getProperty(Constants.REDIS_TYPE);
			purgeFolder = prop.getProperty(Constants.PURGE_FOLDER);

			JedisPoolConfig poolConfig = new JedisPoolConfig();
			poolConfig.setMaxTotal(48);
			jedisPool = new JedisPool(poolConfig, host, port, 10000, pass);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					logger.error(e.getMessage(), e);
				}
			}
		}
	}

	public FileHandler(File file) {
		this.file = file;
	}

	public void run() {
		String line;
		int counter = 0;

		Jedis j = null;
		BufferedReader reader = null;
		try {
			j = jedisPool.getResource();
			reader = new BufferedReader(new FileReader(file));

			try {
				while (counter < MAXLINE && (line = reader.readLine()) != null) {
					if (line.startsWith("KEY:")) {
						String[] s = line.split("\\s+");
						if (s.length > 0) {
							String key = s[1];
							String val = file.getAbsolutePath().substring(purgeFolder.length()+1);
							String[] tmp = val.split("/", 2);

							if ("string".equals(redisType)) {
								j.set(String.format("%s_%s", tmp[0], key), val);
							} else {
								j.hset(tmp[0], key, val);
							}
						}
					}
					counter++;
				}
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			} finally {
				if (reader != null) {
					try {
						reader.close();
					} catch (IOException e) {
						logger.error(e.getMessage(), e);
					}
				}
			}
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		} finally {
			if(j != null){
				j.close();
			}
		}
	}

}
