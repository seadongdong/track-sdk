package com.yonyou.cloud.track;

import java.util.HashMap;
import java.util.Map;


public class Test {

	
	public static void main(String[] args )throws Exception{
		final Track track = new Track(new Track.ConcurrentLoggingConsumer("/Users/BENJAMIN/data/file.log"));
		
		Map<String, Object> per = new HashMap<String,Object>();
		per.put("aaa","AAAAA");
		per.put("bbbb", "BBBB");
		
		track.track("test","123",per);
		
		track.shutdown();
	
	}
}
