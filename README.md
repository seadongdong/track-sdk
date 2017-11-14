# Fork from Sensors

实现项目中用的日志埋点功能，后续通过logstash输出到es中

## Easy Installation

Add the following lines to your project's pom.xml:

```
		 <dependency>
			<groupId>com.yonyou.cloud</groupId>
			<artifactId>track</artifactId>
			<version>1.0.0</version>
		</dependency>
```

## How To Use

```
    final Track track = new Track(new Track.ConcurrentLoggingConsumer("/Users/BENJAMIN/data/file.log"));
		
		Map<String, Object> per = new HashMap<String,Object>();
		per.put("aaa","AAAAA");
		per.put("bbbb", "BBBB");
		
		track.track("test",per);
		
		track.shutdown();
```

## 显示效果

上面的程序执行完后会在对应的目录生成file文件  
内容如下：  
{"lib":{"$lib":"Java","$lib_method":"code","$lib_version":"1.0.0","$lib_detail":"com.yonyou.cloud.track.Test##main##Test.java##17"},"time":1510641026414,"type":"track","event":"test","properties":{"$lib":"Java","aaa":"AAAAA","bbbb":"BBBB","$lib_version":"1.0.0"}}


