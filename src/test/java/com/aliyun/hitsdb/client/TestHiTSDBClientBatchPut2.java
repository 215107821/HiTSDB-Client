package com.aliyun.hitsdb.client;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.aliyun.hitsdb.client.HiTSDB;
import com.aliyun.hitsdb.client.HiTSDBClientFactory;
import com.aliyun.hitsdb.client.HiTSDBConfig;
import com.aliyun.hitsdb.client.callback.BatchPutCallback;
import com.aliyun.hitsdb.client.exception.http.HttpClientInitException;
import com.aliyun.hitsdb.client.value.Result;
import com.aliyun.hitsdb.client.value.request.Point;
import com.aliyun.hitsdb.client.value.type.Aggregator;

public class TestHiTSDBClientBatchPut2 {

	private static int getTime() {
		int time;
		try {
			String strDate = "2017-11-27 11:15:15";
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			time = (int) (sdf.parse(strDate).getTime() / 1000);
		} catch (ParseException e) {
			e.printStackTrace();
			time = 0;
		}
		return time;

	}

	HiTSDB tsdb;

	@Before
	public void init() throws HttpClientInitException {
		BatchPutCallback pcb = new BatchPutCallback() {

			final AtomicInteger num = new AtomicInteger();

			@Override
			public void failed(String address, List<Point> points, Exception ex) {
				System.err.println("业务回调出错！" + points.size() + " error!");
				ex.printStackTrace();
			}

			@Override
			public void response(String address, List<Point> input, Result output) {
				int count = num.addAndGet(input.size());
				System.out.println("已处理" + count + "个点");
			}

		};

		HiTSDBConfig config = HiTSDBConfig.address("10.100.23.3", 24242).listenBatchPut(pcb).httpConnectTimeout(3).batchPutRetryCount(2).ioThreadCount(10).config();
		tsdb = HiTSDBClientFactory.connect(config);
	}

	@After
	public void after() {
		try {
			System.out.println("将要关闭");
			tsdb.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testPutData() throws Exception {
		try {

			String strDate = "2016-12-12 18:03:00";
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			int time = (int) (sdf.parse(strDate).getTime() / 1000);

			Point point = Point.metric("zhk.order.cnt").tag("supplierId", "2088122648957122").timestamp(time).value(10).build();
			tsdb.put(point);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * 2017-11-27 17:00:00~18:00:00 每一秒钟插入一条0~2500的随机数
	 * 
	 * @throws Exception
	 */
	@Test
	public void testPutData2() throws Exception {
		long start = System.currentTimeMillis();

		String strDate = "2017-11-27 11:00:00";
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		int time = (int) (sdf.parse(strDate).getTime() / 1000);

		for (int i = 0; i < 3600 * 11; i++) {
			Point point = Point.metric("zhk.order.cnt").tag("supplierId", "2088122648957121").tag("shopId", "2016060600077000000015845615").tag("regionId", "110100").tag("appId", "2")
					.timestamp(time + i).value(new Random().nextInt(100)).build();
			tsdb.put(point);
			Thread.sleep(1);

			Point point2 = Point.metric("zhk.order.cnt").tag("supplierId", "2088122648957121").tag("shopId", "2016060600077000000015845615").tag("regionId", "110100").tag("appId", "4")
					.timestamp(time + i).value(new Random().nextInt(150)).build();
			tsdb.put(point2);
			Thread.sleep(2);

			Point point3 = Point.metric("zhk.order.cnt").tag("supplierId", "2088122648957121").tag("shopId", "2016060600077000000015845615").tag("regionId", "110100").tag("appId", "7")
					.timestamp(time + i).value(new Random().nextInt(50)).build();
			tsdb.put(point3);
			Thread.sleep(3);

			Point point4 = Point.metric("zhk.order.amt").tag("supplierId", "2088122648957121").tag("shopId", "2016060600077000000015845615").tag("regionId", "110100").tag("appId", "2")
					.timestamp(time + i).value(new BigDecimal(new Random().nextDouble() * 20).setScale(2, BigDecimal.ROUND_HALF_UP)).build();
			tsdb.put(point4);
			Thread.sleep(1);

			Point point5 = Point.metric("zhk.order.amt").tag("supplierId", "2088122648957121").tag("shopId", "2016060600077000000015845615").tag("regionId", "110100").tag("appId", "4")
					.timestamp(time + i).value(new BigDecimal(new Random().nextDouble() * 50).setScale(2, BigDecimal.ROUND_HALF_UP)).build();
			tsdb.put(point5);
			Thread.sleep(2);

			Point point6 = Point.metric("zhk.order.amt").tag("supplierId", "2088122648957121").tag("shopId", "2016060600077000000015845615").tag("regionId", "110100").tag("appId", "7")
					.timestamp(time + i).value(new BigDecimal(new Random().nextDouble() * 90).setScale(2, BigDecimal.ROUND_HALF_UP)).build();
			tsdb.put(point6);
			Thread.sleep(3);
		}

		System.out.println((System.currentTimeMillis() - start) / 60000 + " 分钟");

	}

	@Test
	public void testPutData3() throws Exception {
		long start = System.currentTimeMillis();

		String strDate = "2017-12-01 10:00:00";
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		int time = (int) (sdf.parse(strDate).getTime() / 1000);

		for (int i = 0; i < 3600 * 14; i++) {
			Point point = Point.metric("zhk.order.amt").tag("supplierId", "2088122648957121").tag("shopId", "2016060600077000000015845615").tag("regionId", "110100").tag("appId", "2")
					.timestamp(time + i).value(new BigDecimal(new Random().nextDouble() * 250).setScale(2, BigDecimal.ROUND_HALF_UP)).build();
			tsdb.put(point);
			Thread.sleep(2);

			Point point2 = Point.metric("zhk.order.amt").tag("supplierId", "2088122648957121").tag("shopId", "2016060600077000000015845615").tag("regionId", "110100").tag("appId", "4")
					.timestamp(time + i).value(new BigDecimal(new Random().nextDouble() * 50).setScale(2, BigDecimal.ROUND_HALF_UP)).build();
			tsdb.put(point2);
			Thread.sleep(2);

			Point point3 = Point.metric("zhk.order.amt").tag("supplierId", "2088122648957121").tag("shopId", "2016060600077000000015845615").tag("regionId", "110100").tag("appId", "7")
					.timestamp(time + i).value(new BigDecimal(new Random().nextDouble() * 150).setScale(2, BigDecimal.ROUND_HALF_UP)).build();
			tsdb.put(point3);
			Thread.sleep(5);
		}

		System.out.println((System.currentTimeMillis() - start) / 60000 + " 分钟");

	}

	@Test
	public void testLargeDateBatchPutDataCallback() {
		Random random = new Random();
		int time = getTime();
		for (int i = 0; i < 100000; i++) {
			double nextDouble = random.nextDouble() * 100;
			Point point = Point.metric("test1").tag("tagk1", "tagv1").tag("tagk2", "tagv2").tag("tagk3", "tagv3").timestamp(time + i).value(nextDouble).build();
			tsdb.put(point);
		}
	}

	@Test
	public void testMiddleDateBatchPutDataCallback() {
		Random random = new Random();
		int time = getTime();
		for (int i = 0; i < 5500; i++) {
			double nextDouble = random.nextDouble() * 100;
			Point point = Point.metric("test1").tag("tagk1", "tagv1").tag("tagk2", "tagv2").tag("tagk3", "tagv3").timestamp(time + i).value(nextDouble).build();
			tsdb.put(point);
		}
	}

	@Test
	public void testLitterDateBatchPutDataCallback() {
		Random random = new Random();
		int time = getTime();
		for (int i = 0; i < 4000; i++) {
			double nextDouble = random.nextDouble() * 100;
			Point point = Point.metric("test1").tag("tagk1", "tagv1").tag("tagk2", "tagv2").tag("tagk3", "tagv3").timestamp(time + i).value(nextDouble).build();
			tsdb.put(point);
		}
	}

	public static void main(String[] args) throws Exception {
		System.out.println(new Random().nextDouble() * 100);
		System.out.println(new BigDecimal(new Random().nextDouble() * 100).setScale(2, BigDecimal.ROUND_HALF_UP));

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String strDate = sdf.format(new Date());
		int time = (int) (sdf.parse(sdf.format(new Date()).substring(0, strDate.length() - 2).concat("00")).getTime() / 1000);
		System.err.println(time);

	}
}
