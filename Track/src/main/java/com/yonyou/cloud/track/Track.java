package com.yonyou.cloud.track;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.yonyou.cloud.exceptions.InvalidArgumentException;
import com.yonyou.cloud.track.util.Base64Coder;

/**
 *  埋点客户端
 * @author daniell
 *
 */
public class Track {
	private static final Logger loger = LoggerFactory.getLogger(Track.class);
	private static final String TRACK ="track";
	private static final String TRACK_SIGNUP ="track_signup";
	private static final String TIME ="$time";
	private static final String APP_VERSION ="$app_version";
	private boolean enableTimeFree = false;

	public boolean isEnableTimeFree() {
		return enableTimeFree;
	}

	public void setEnableTimeFree(boolean enableTimeFree) {
		this.enableTimeFree = enableTimeFree;
	}

	private interface Consumer {
		/**
		 * 写入信息
		 * @param message
		 */
		void send(Map<String, Object> message);

		/**
		 * 立即发送缓存中的所有日志
		 */
		void flush();

		/**
		 * 停止SensorsDataAPI所有线程，API停止前会清空所有本地数据
		 */
		void close();
	}


	public static class ConcurrentLoggingConsumer extends InnerLoggingConsumer {

		public ConcurrentLoggingConsumer(final String filenamePrefix) throws IOException {
			this(filenamePrefix, 8192);
		}

		public ConcurrentLoggingConsumer(String filenamePrefix, int bufferSize) throws IOException {
			super(new LoggingFileWriterFactory() {
				@Override
				public LoggingFileWriter getFileWriter(String fileName, String scheduleFileName)
						throws FileNotFoundException {
					return new ConcurrentLoggingConsumer.InnerLoggingFileWriter(scheduleFileName);
				}
			}, filenamePrefix, bufferSize);
		}

		static class InnerLoggingFileWriter implements LoggingFileWriter {

			private final String fileName;
			private final FileOutputStream outputStream;

			InnerLoggingFileWriter(final String fileName) throws FileNotFoundException {
				this.outputStream = new FileOutputStream(fileName, true);
				this.fileName = fileName;
			}
			@Override
			public void close() {
				try {
					outputStream.close();
				} catch (Exception e) {
					throw new RuntimeException("fail to close output stream.", e);
				}
			}
			@Override
			public boolean isValid(final String fileName) {
				return this.fileName.equals(fileName);
			}
			@Override
			public boolean write(final StringBuilder sb) {
				FileLock lock = null;
				try {
					final FileChannel channel = outputStream.getChannel();
					lock = channel.lock(0, Long.MAX_VALUE, false);
					outputStream.write(sb.toString().getBytes("UTF-8"));
				} catch (Exception e) {
					throw new RuntimeException("fail to write file.", e);
				} finally {
					if (lock != null) {
						try {
							lock.release();
						} catch (IOException e) {
							throw new RuntimeException("fail to release file lock.", e);
						}
					}
				}

				return true;
			}
		}

	}

	interface LoggingFileWriter {
		/**
		 * 文件名验证
		 * @param fileName
		 * @return
		 */
		boolean isValid(final String fileName);

		/**
		 * 文件流写入
		 * @param sb
		 * @return
		 */
		boolean write(final StringBuilder sb);

		/**
		 * 文件流关闭
		 */
		void close();
	}

	interface LoggingFileWriterFactory {
		/**
		 * 向文件末尾追加内容
		 * @param fileName
		 * @param scheduleFileName
		 * @return
		 * @throws FileNotFoundException
		 */
		LoggingFileWriter getFileWriter(final String fileName, final String scheduleFileName)
				throws FileNotFoundException;
	}

	static class InnerLoggingConsumer implements Consumer {
		/**
		 *  1G
		 */
		private final static int BUFFER_LIMITATION = 1 * 1024 * 1024 * 1024; 

		private final ObjectMapper jsonMapper;
		private final String filenamePrefix;
		private final StringBuilder messageBuffer;
		private final int bufferSize;
		private final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

		private final LoggingFileWriterFactory fileWriterFactory;
		private LoggingFileWriter fileWriter;

		public InnerLoggingConsumer(LoggingFileWriterFactory fileWriterFactory, String filenamePrefix, int bufferSize)
				throws IOException {
			this.fileWriterFactory = fileWriterFactory;
			this.filenamePrefix = filenamePrefix;
			this.jsonMapper = getJsonObjectMapper();
			this.messageBuffer = new StringBuilder(bufferSize);
			this.bufferSize = bufferSize;
		}

		@Override
		public synchronized void send(Map<String, Object> message) {
			if (messageBuffer.length() < BUFFER_LIMITATION) {
				try {
					jsonMapper.setSerializationInclusion(Include.NON_NULL);  
					messageBuffer.append(jsonMapper.writeValueAsString(message));
					messageBuffer.append("\n");
				} catch (JsonProcessingException e) {
					throw new RuntimeException("fail to process json", e);
				}
			} else {
				throw new RuntimeException("logging buffer exceeded the allowed limitation.");
			}

			if (messageBuffer.length() >= bufferSize) {
				flush();
			}
		}

		private String constructFileName(Date now) {
			return filenamePrefix + "." + simpleDateFormat.format(now);
		}

		@Override
		public synchronized void flush() {
			if (messageBuffer.length() == 0) {
				return;
			}

			String filename = constructFileName(new Date());

			if (fileWriter != null && !fileWriter.isValid(filename)) {
				fileWriter.close();
				fileWriter = null;
			}

			if (fileWriter == null) {
				try {
					fileWriter = this.fileWriterFactory.getFileWriter(filenamePrefix, filename);
				} catch (FileNotFoundException e) {
					throw new RuntimeException(e);
				}
			}

			if (fileWriter.write(messageBuffer)) {
				messageBuffer.setLength(0);
			}
		}

		@Override
		public synchronized void close() {
			flush();

			fileWriter.close();
			fileWriter = null;
		}
	}

	public Track(final Consumer consumer) {
		this.consumer = consumer;

		this.superProperties = new ConcurrentHashMap<String, Object>();
		clearSuperProperties();
	}

	/**
	 * 设置每个事件都带有的一些公共属性
	 *
	 * 当track的Properties，superProperties和SDK自动生成的automaticProperties有相同的key时，
	 * 遵循如下的优先级： track.properties 高于 superProperties 高于 automaticProperties
	 *
	 * 另外，当这个接口被多次调用时，是用新传入的数据去merge先前的数据
	 *
	 * 例如，在调用接口前，dict是 {"a":1, "b": "bbb"}，传入的dict是 {"b": 123, "c":
	 * "asd"}，则merge后 的结果是 {"a":1, "b": 123, "c": "asd"}
	 *
	 * @param superPropertiesMap
	 *            一个或多个公共属性
	 */
	public void registerSuperProperties(Map<String, Object> superPropertiesMap) {
		for (Map.Entry<String, Object> item : superPropertiesMap.entrySet()) {
			this.superProperties.put(item.getKey(), item.getValue());
		}
	}

	/**
	 * 清除公共属性
	 */
	public void clearSuperProperties() {
		this.superProperties.clear();
		this.superProperties.put("$lib", "Java");
		this.superProperties.put("$lib_version", SDK_VERSION);
	}

//	/**
//	 * 记录一个没有任何属性的事件
//	 *
//	 * @param distinctId
//	 *            用户 ID
//	 * @param isLoginId
//	 *            用户 ID 是否是登录 ID，false 表示该 ID 是一个匿名 ID
//	 * @param eventName
//	 *            事件名称
//	 *
//	 * @throws InvalidArgumentException
//	 *             eventName 或 properties 不符合命名规范和类型规范时抛出该异常
//	 */
//	public void track(String distinctId, boolean isLoginId, String eventName) throws InvalidArgumentException {
//		addEvent(distinctId, isLoginId, null, "track", eventName, null);
//	}

	/**
	 * 记录一个拥有一个或多个属性的事件。属性取值可接受类型为{@link Number}, {@link String}, {@link Date}和
	 * {@link List}
	 *
	 * @param eventName
	 *            事件名称
	 * @param bizType
	 *            事件业务类型
	 * @param properties
	 *            事件的属性
	 *
	 * @throws InvalidArgumentException
	 *             eventName 或 properties 不符合命名规范和类型规范时抛出该异常
	 */
	public void track(String eventName,String bizType, Map<String, Object> properties)
			throws InvalidArgumentException {
		addEvent(null, false, null, bizType, eventName, properties);
	}

//	/**
//	 * 记录用户注册事件
//	 *
//	 * 这个接口是一个较为复杂的功能，请在使用前先阅读相关说明:
//	 * http://www.sensorsdata.cn/manual/track_signup.html 并在必要时联系我们的技术支持人员。
//	 *
//	 * @param loginId
//	 *            登录 ID
//	 * @param anonymousId
//	 *            匿名 ID
//	 *
//	 * @throws InvalidArgumentException
//	 *             eventName 或 properties 不符合命名规范和类型规范时抛出该异常
//	 */
//	public void trackSignUp(String loginId, String anonymousId) throws InvalidArgumentException {
//		addEvent(loginId, false, anonymousId, "track_signup", "$SignUp", null);
//	}

//	/**
//	 * 记录用户注册事件
//	 *
//	 * 这个接口是一个较为复杂的功能，请在使用前先阅读相关说明:
//	 * http://www.sensorsdata.cn/manual/track_signup.html 并在必要时联系我们的技术支持人员。
//	 * <p>
//	 * 属性取值可接受类型为{@link Number}, {@link String}, {@link Date}和{@link List}，若属性包
//	 * 含 $time 字段，它会覆盖事件的默认时间属性，该字段只接受{@link Date}类型
//	 *
//	 * @param loginId
//	 *            登录 ID
//	 * @param anonymousId
//	 *            匿名 ID
//	 * @param properties
//	 *            事件的属性
//	 *
//	 * @throws InvalidArgumentException
//	 *             eventName 或 properties 不符合命名规范和类型规范时抛出该异常
//	 */
//	public void trackSignUp(String loginId, String anonymousId, Map<String, Object> properties)
//			throws InvalidArgumentException {
//		addEvent(loginId, false, anonymousId, "track_signup", "$SignUp", properties);
//	}

	/**
	 * 设置用户的属性。属性取值可接受类型为{@link Number}, {@link String}, {@link Date}和
	 * {@link List}， 若属性包含 $time 字段，则它会覆盖事件的默认时间属性，该字段只接受{@link Date}类型
	 *
	 * 如果要设置的properties的key，之前在这个用户的profile中已经存在，则覆盖，否则，新创建
	 *
	 * @param distinctId
	 *            用户 ID
	 * @param isLoginId
	 *            用户 ID 是否是登录 ID，false 表示该 ID 是一个匿名 ID
	 * @param properties
	 *            用户的属性
	 *
	 * @throws InvalidArgumentException
	 *             eventName 或 properties 不符合命名规范和类型规范时抛出该异常
	 */
	public void profileSet(String distinctId, boolean isLoginId, Map<String, Object> properties)
			throws InvalidArgumentException {
		addEvent(distinctId, isLoginId, null, "profile_set", null, properties);
	}

	/**
	 * 设置用户的属性。这个接口只能设置单个key对应的内容，同样，如果已经存在，则覆盖，否则，新创建
	 *
	 * @param distinctId
	 *            用户 ID
	 * @param isLoginId
	 *            用户 ID 是否是登录 ID，false 表示该 ID 是一个匿名 ID
	 * @param property
	 *            属性名称
	 * @param value
	 *            属性的值
	 *
	 * @throws InvalidArgumentException
	 *             eventName 或 properties 不符合命名规范和类型规范时抛出该异常
	 */
	public void profileSet(String distinctId, boolean isLoginId, String property, Object value)
			throws InvalidArgumentException {
		Map<String, Object> properties = new HashMap<String, Object>();
		properties.put(property, value);
		addEvent(distinctId, isLoginId, null, "profile_set", null, properties);
	}

	/**
	 * 首次设置用户的属性。 属性取值可接受类型为{@link Number}, {@link String}, {@link Date}和
	 * {@link List}， 若属性包含 $time 字段，则它会覆盖事件的默认时间属性，该字段只接受{@link Date}类型
	 *
	 * 与profileSet接口不同的是： 如果要设置的properties的key，在这个用户的profile中已经存在，则不处理，否则，新创建
	 *
	 * @param distinctId
	 *            用户 ID
	 * @param isLoginId
	 *            用户 ID 是否是登录 ID，false 表示该 ID 是一个匿名 ID
	 * @param properties
	 *            用户的属性
	 *
	 * @throws InvalidArgumentException
	 *             eventName 或 properties 不符合命名规范和类型规范时抛出该异常
	 */
	public void profileSetOnce(String distinctId, boolean isLoginId, Map<String, Object> properties)
			throws InvalidArgumentException {
		addEvent(distinctId, isLoginId, null, "profile_set_once", null, properties);
	}

	/**
	 * 首次设置用户的属性。这个接口只能设置单个key对应的内容。
	 * 与profileSet接口不同的是，如果key的内容之前已经存在，则不处理，否则，重新创建
	 *
	 * @param distinctId
	 *            用户 ID
	 * @param isLoginId
	 *            用户 ID 是否是登录 ID，false 表示该 ID 是一个匿名 ID
	 * @param property
	 *            属性名称
	 * @param value
	 *            属性的值
	 *
	 * @throws InvalidArgumentException
	 *             eventName 或 properties 不符合命名规范和类型规范时抛出该异常
	 */
	public void profileSetOnce(String distinctId, boolean isLoginId, String property, Object value)
			throws InvalidArgumentException {
		Map<String, Object> properties = new HashMap<String, Object>();
		properties.put(property, value);
		addEvent(distinctId, isLoginId, null, "profile_set_once", null, properties);
	}

	/**
	 * 为用户的一个或多个数值类型的属性累加一个数值，若该属性不存在，则创建它并设置默认值为0。属性取值只接受 {@link Number}类型
	 *
	 * @param distinctId
	 *            用户 ID
	 * @param isLoginId
	 *            用户 ID 是否是登录 ID，false 表示该 ID 是一个匿名 ID
	 * @param properties
	 *            用户的属性
	 *
	 * @throws InvalidArgumentException
	 *             eventName 或 properties 不符合命名规范和类型规范时抛出该异常
	 */
	public void profileIncrement(String distinctId, boolean isLoginId, Map<String, Object> properties)
			throws InvalidArgumentException {
		addEvent(distinctId, isLoginId, null, "profile_increment", null, properties);
	}

	/**
	 * 为用户的数值类型的属性累加一个数值，若该属性不存在，则创建它并设置默认值为0
	 *
	 * @param distinctId
	 *            用户 ID
	 * @param isLoginId
	 *            用户 ID 是否是登录 ID，false 表示该 ID 是一个匿名 ID
	 * @param property
	 *            属性名称
	 * @param value
	 *            属性的值
	 *
	 * @throws InvalidArgumentException
	 *             eventName 或 properties 不符合命名规范和类型规范时抛出该异常
	 */
	public void profileIncrement(String distinctId, boolean isLoginId, String property, long value)
			throws InvalidArgumentException {
		Map<String, Object> properties = new HashMap<String, Object>();
		properties.put(property, value);
		addEvent(distinctId, isLoginId, null, "profile_increment", null, properties);
	}

	/**
	 * 为用户的一个或多个数组类型的属性追加字符串，属性取值类型必须为 {@link java.util.List}，且列表中元素的类型 必须为
	 * {@link java.lang.String}
	 *
	 * @param distinctId
	 *            用户 ID
	 * @param isLoginId
	 *            用户 ID 是否是登录 ID，false 表示该 ID 是一个匿名 ID
	 * @param properties
	 *            用户的属性
	 *
	 * @throws InvalidArgumentException
	 *             eventName 或 properties 不符合命名规范和类型规范时抛出该异常
	 */
	public void profileAppend(String distinctId, boolean isLoginId, Map<String, Object> properties)
			throws InvalidArgumentException {
		addEvent(distinctId, isLoginId, null, "profile_append", null, properties);
	}

	/**
	 * 为用户的数组类型的属性追加一个字符串
	 *
	 * @param distinctId
	 *            用户 ID
	 * @param isLoginId
	 *            用户 ID 是否是登录 ID，false 表示该 ID 是一个匿名 ID
	 * @param property
	 *            属性名称
	 * @param value
	 *            属性的值
	 *
	 * @throws InvalidArgumentException
	 *             eventName 或 properties 不符合命名规范和类型规范时抛出该异常
	 */
	public void profileAppend(String distinctId, boolean isLoginId, String property, String value)
			throws InvalidArgumentException {
		List<String> values = new ArrayList<String>();
		values.add(value);
		Map<String, Object> properties = new HashMap<String, Object>();
		properties.put(property, values);
		addEvent(distinctId, isLoginId, null, "profile_append", null, properties);
	}

	/**
	 * 删除用户某一个属性
	 *
	 * @param distinctId
	 *            用户 ID
	 * @param isLoginId
	 *            用户 ID 是否是登录 ID，false 表示该 ID 是一个匿名 ID
	 * @param property
	 *            属性名称
	 *
	 * @throws InvalidArgumentException
	 *             eventName 或 properties 不符合命名规范和类型规范时抛出该异常
	 */
	public void profileUnset(String distinctId, boolean isLoginId, String property) throws InvalidArgumentException {
		Map<String, Object> properties = new HashMap<String, Object>();
		properties.put(property, true);
		addEvent(distinctId, isLoginId, null, "profile_unset", null, properties);
	}

	/**
	 * 删除用户所有属性
	 *
	 * @param distinctId
	 *            用户 ID
	 * @param isLoginId
	 *            用户 ID 是否是登录 ID，false 表示该 ID 是一个匿名 ID
	 *
	 * @throws InvalidArgumentException
	 *             distinctId 不符合命名规范时抛出该异常
	 */
	public void profileDelete(String distinctId, boolean isLoginId) throws InvalidArgumentException {
		addEvent(distinctId, isLoginId, null, "profile_delete", null, new HashMap<String, Object>());
	}

	/**
	 * 立即发送缓存中的所有日志
	 */
	public void flush() {
		this.consumer.flush();
	}

	/**
	 * 停止SensorsDataAPI所有线程，API停止前会清空所有本地数据
	 */
	public void shutdown() {
		this.consumer.close();
	}

	@SuppressWarnings("unused")
	private static class HttpConsumer {

		@SuppressWarnings("serial")
		static class HttpConsumerException extends Exception {

			HttpConsumerException(String error, String sendingData, int httpStatusCode, String httpContent) {
				super(error);
				this.sendingData = sendingData;
				this.httpStatusCode = httpStatusCode;
				this.httpContent = httpContent;
			}

			@SuppressWarnings("unused")
			String getSendingData() {
				return sendingData;
			}

			@SuppressWarnings("unused")
			int getHttpStatusCode() {
				return httpStatusCode;
			}

			@SuppressWarnings("unused")
			String getHttpContent() {
				return httpContent;
			}

			final String sendingData;
			final int httpStatusCode;
			final String httpContent;
		}

		@SuppressWarnings("unused")
		HttpConsumer(String serverUrl, Map<String, String> httpHeaders) {
			this.serverUrl = serverUrl;
			this.httpHeaders = httpHeaders;

			this.compressData = true;
		}

		@SuppressWarnings("unused")
		HttpResponse consume(final String data) throws IOException, HttpConsumerException {
			HttpResponse response = new DefaultHttpClient().execute(getHttpRequest(data));
			int httpStatusCodeMax=200;
			int httpStatusCodeMin=300;
			int httpStatusCode = response.getStatusLine().getStatusCode();
			if (httpStatusCode < httpStatusCodeMax || httpStatusCode >= httpStatusCodeMin) {
				String httpContent = EntityUtils.toString(response.getEntity(), "UTF-8");
				throw new HttpConsumerException(String.format("Unexpected response %d from Sensors " + "Analytics: %s",
						httpStatusCode, httpContent), data, httpStatusCode, httpContent);
			}

			return response;
		}

		HttpUriRequest getHttpRequest(final String data) throws IOException {
			HttpPost httpPost = new HttpPost(this.serverUrl);

			httpPost.setEntity(getHttpEntry(data));
			httpPost.addHeader("User-Agent", "SensorsAnalytics Java SDK");

			if (this.httpHeaders != null) {
				for (Map.Entry<String, String> entry : this.httpHeaders.entrySet()) {
					httpPost.addHeader(entry.getKey(), entry.getValue());
				}
			}

			return httpPost;
		}

		UrlEncodedFormEntity getHttpEntry(final String data) throws IOException {
			byte[] bytes = data.getBytes(Charset.forName("UTF-8"));

			List<NameValuePair> nameValuePairs = new ArrayList<NameValuePair>();

			if (compressData) {
				ByteArrayOutputStream os = new ByteArrayOutputStream(bytes.length);
				GZIPOutputStream gos = new GZIPOutputStream(os);
				gos.write(bytes);
				gos.close();
				byte[] compressed = os.toByteArray();
				os.close();

				nameValuePairs.add(new BasicNameValuePair("gzip", "1"));
				nameValuePairs.add(new BasicNameValuePair("data_list", new String(Base64Coder.encode(compressed))));
			} else {
				nameValuePairs.add(new BasicNameValuePair("gzip", "0"));
				nameValuePairs.add(new BasicNameValuePair("data_list", new String(Base64Coder.encode(bytes))));
			}

			return new UrlEncodedFormEntity(nameValuePairs);
		}

		final String serverUrl;
		final Map<String, String> httpHeaders;

		final Boolean compressData;
	}

	private void addEvent(String distinctId, boolean isLoginId, String originDistinceId, String actionType,
			String eventName, Map<String, Object> properties) throws InvalidArgumentException {
//		assertKey("Distinct Id", distinctId);
		assertProperties(actionType, properties);
		if(TRACK.equals(actionType)) {
			assertKeyWithRegex("Event Name", eventName);
		} else if (TRACK_SIGNUP.equals(actionType)) {
			assertKey("Original Distinct Id", originDistinceId);
		}

		// Event time
		long time = System.currentTimeMillis();
		if (properties != null && properties.containsKey(TIME)) {
			Date eventTime = (Date) properties.get("$time");
			properties.remove("$time");
			time = eventTime.getTime();
		}

		Map<String, Object> eventProperties = new HashMap<String, Object>();
		if (TRACK.equals(actionType) || TRACK_SIGNUP.equals(actionType)) {
			eventProperties.putAll(superProperties);
		}
		if (properties != null) {
			eventProperties.putAll(properties);
		}

//		if (isLoginId) {
//			eventProperties.put("$is_login_id", true);
//		}

		Map<String, String> libProperties = getLibProperties();

		Map<String, Object> event = new HashMap<String, Object>();

		event.put("type", actionType);
		event.put("time", time);
//		event.put("distinct_id", distinctId);
		event.put("properties", eventProperties);
		event.put("lib", libProperties);

		if (enableTimeFree) {
			event.put("time_free", true);
		}

		if (TRACK.equals(actionType)) {
			event.put("event", eventName);
		} else if (TRACK_SIGNUP.equals(actionType)) {
			event.put("event", eventName);
			event.put("original_id", originDistinceId);
		}

		this.consumer.send(event);
	}

	private Map<String, String> getLibProperties() {
		int traceLength=3;
		Map<String, String> libProperties = new HashMap<String, String>();
		libProperties.put("$lib", "Java");
		libProperties.put("$lib_version", SDK_VERSION);
		libProperties.put("$lib_method", "code");

		if (this.superProperties.containsKey(APP_VERSION)) {
			libProperties.put("$app_version", (String) this.superProperties.get("$app_version"));
		}

		StackTraceElement[] trace = (new Exception()).getStackTrace();
		if (trace.length > traceLength) {
			StackTraceElement traceElement = trace[3];
			libProperties.put("$lib_detail", String.format("%s##%s##%s##%s", traceElement.getClassName(),
					traceElement.getMethodName(), traceElement.getFileName(), traceElement.getLineNumber()));
		}

		return libProperties;
	}

	private void assertKey(String type, String key) throws InvalidArgumentException {
		int keyLengthMax=255;
		if (key == null || key.length() < 1) {
			throw new InvalidArgumentException("The " + type + " is empty.");
		}
		if (key.length() > keyLengthMax) {
			throw new InvalidArgumentException("The " + type + " is too long, max length is 255.");
		}
	}

	private void assertKeyWithRegex(String type, String key) throws InvalidArgumentException {
		assertKey(type, key);
		if (!(KEY_PATTERN.matcher(key).matches())) {
			throw new InvalidArgumentException("The " + type + "'" + key + "' is invalid.");
		}
	}

	@SuppressWarnings("unchecked")
	private void assertProperties(String eventType, Map<String, Object> properties) throws InvalidArgumentException {
		if (null == properties) {
			return;
		}
		for (Map.Entry<String, Object> property : properties.entrySet()) {
			if ("$is_login_id".equals(property.getKey())) {
				if (!(property.getValue() instanceof Boolean)) {
					throw new InvalidArgumentException("The property value of '$is_login_id' should be " + "Boolean.");
				}
				continue;
			}

			assertKeyWithRegex("property", property.getKey());

			if (!(property.getValue() instanceof Number) && !(property.getValue() instanceof Date)
					&& !(property.getValue() instanceof String) && !(property.getValue() instanceof Boolean)
					&& !(property.getValue() instanceof List<?>)) {
//				throw new InvalidArgumentException("The property '" + property.getKey() + "' should be a basic type: "
//						+ "Number, String, Date, Boolean, List<String>.");
				//标记点 注释掉
				loger.info("The property '" + property.getKey() + "' not is a basic type: "
						+ "Number, String, Date, Boolean, List<String>.");
			}

			if ("$time".equals(property.getKey()) && !(property.getValue() instanceof Date)) {
				throw new InvalidArgumentException("The property '$time' should be a java.util.Date.");
			}

			// List 类型的属性值，List 元素必须为 String 类型
			if (property.getValue() instanceof List<?>) {
				for (final ListIterator<Object> it = ((List<Object>) property.getValue()).listIterator(); it
						.hasNext();) {
					Object element = it.next();
					if (!(element instanceof String)) {
						throw new InvalidArgumentException(
								"The property '" + property.getKey() + "' should be a list of String.");
					}
					if (((String) element).length() > 8192) {
						it.set(((String) element).substring(0, 8192));
					}
				}
			}

			// String 类型的属性值，长度不能超过 8192
			if (property.getValue() instanceof String) {
				 
				String value = (String) property.getValue();
				 
				if (value.length() > 8192) {
					property.setValue(value.substring(0, 8192));
				}
				 
			}

			if ("profile_increment".equals(eventType)) {
				if (!(property.getValue() instanceof Number)) {
					throw new InvalidArgumentException(
							"The property value of PROFILE_INCREMENT should be a " + "Number.");
				}
			} else if ("profile_append".equals(eventType)) {
				if (!(property.getValue() instanceof List<?>)) {
					throw new InvalidArgumentException(
							"The property value of PROFILE_INCREMENT should be a " + "List<String>.");
				}
			}
		}
	}

	@SuppressWarnings("unused")
	private static String strJoin(String[] arr, String sep) {
		StringBuilder sbStr = new StringBuilder();
		for (int i = 0, il = arr.length; i < il; i++) {
			if (i > 0) {
				sbStr.append(sep);
			}
			sbStr.append(arr[i]);
		}
		return sbStr.toString();
	}

	private static ObjectMapper getJsonObjectMapper() {
		ObjectMapper jsonObjectMapper = new ObjectMapper();
		// 容忍json中出现未知的列
		jsonObjectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		// 兼容java中的驼峰的字段名命名
		jsonObjectMapper.setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);
		jsonObjectMapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"));
		return jsonObjectMapper;
	}

	private final static String SDK_VERSION = "1.0.0";

	private final static Pattern KEY_PATTERN = Pattern.compile(
			"^((?!^distinct_id$|^original_id$|^time$|^properties$|^id$|^first_id$|^second_id$|^users$|^events$|^event$|^user_id$|^date$|^datetime$)[a-zA-Z_$][a-zA-Z\\d_$]{0,99})$",
			Pattern.CASE_INSENSITIVE);

	private final Consumer consumer;

	private final Map<String, Object> superProperties;

}
