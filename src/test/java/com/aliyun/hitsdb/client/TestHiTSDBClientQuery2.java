package com.aliyun.hitsdb.client;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.aliyun.hitsdb.client.callback.QueryCallback;
import com.aliyun.hitsdb.client.exception.http.HttpClientInitException;
import com.aliyun.hitsdb.client.exception.http.HttpUnknowStatusException;
import com.aliyun.hitsdb.client.value.request.Query;
import com.aliyun.hitsdb.client.value.request.SubQuery;
import com.aliyun.hitsdb.client.value.response.QueryResult;
import com.aliyun.hitsdb.client.value.type.Aggregator;

public class TestHiTSDBClientQuery2 {

	HiTSDB tsdb;

	@Before
	public void init() throws HttpClientInitException {
//		HiTSDBConfig config = HiTSDBConfig.address("172.18.10.52", 10801).config();
		HiTSDBConfig config = HiTSDBConfig.address("10.100.23.3", 24242).config();
		tsdb = HiTSDBClientFactory.connect(config);
	}

	@After
	public void after() {
		try {
			tsdb.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testQuery() throws Exception {
		String startDate = "2016-12-12 18:00:00";
		String endDate = "2018-12-12 18:04:59";
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		int t0 = (int) (sdf.parse(startDate).getTime() / 1000);
		int t1 = (int) (sdf.parse(endDate).getTime() / 1000);

		SubQuery subQuery = SubQuery.metric("zhk.order.cnt", Aggregator.SUM).downsample("5m-sum").build();
		Query query = Query.timeRange(sdf.parse(startDate).getTime(), sdf.parse(endDate).getTime()).sub(subQuery).build();

		try {
			System.out.println(query.toJSON());
			List<QueryResult> result = tsdb.query(query);
			System.out.println("查询结果：" + result);
		} catch (HttpUnknowStatusException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testQuery3() throws Exception {
		String startDate = "2017-12-19 16:00:00";
		String endDate = "2017-12-19 16:29:59";
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		int t0 = (int) (sdf.parse(startDate).getTime() / 1000);
		int t1 = (int) (sdf.parse(endDate).getTime() / 1000);

		SubQuery subQuery = SubQuery.metric("zhk.order.cnt", Aggregator.SUM).downsample("1m-sum").build();
		Query query = Query.timeRange(t0, t1).sub(subQuery).build();

		try {
			System.out.println(query.toJSON());
			List<QueryResult> result = tsdb.query(query);
			System.out.println("查询结果：" + result);
		} catch (HttpUnknowStatusException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testQuery2() throws Exception {
		String startDate = "2017-11-27 15:00:00";
		String endDate = "2017-11-27 16:00:00";
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		int t0 = (int) (sdf.parse(startDate).getTime() / 1000);
		int t1 = (int) (sdf.parse(endDate).getTime() / 1000);

		Query query = Query.timeRange(t0, t1).sub(SubQuery.metric("zhk.order.cnt", Aggregator.SUM)
				.tag("supplierId", "2088122648957121").tag("shopId", "2016060600077000000015845615").build()).build();

		System.out.println(query.toJSON());
		List<QueryResult> result = tsdb.query(query);
		System.out.println(result);
		System.out.println("=======1=======");
		
//		Query query2 = Query.timeRange(t0, t1).sub(SubQuery.metric("zhk.order.cnt", Aggregator.SUM)
//				.tag("supplierId", "2088122648957121").tag("shopId", "2016060600077000000015845616").build()).build();
//
//		System.out.println(query2.toJSON());
//		List<QueryResult> result2 = tsdb.query(query2);
//		System.out.println(result2);
//		System.out.println("========2======");
//		
		Query query3 = Query.timeRange(t0, t1).sub(SubQuery.metric("zhk.order.cnt", Aggregator.SUM).downsample("1m-sum")
				.tag("supplierId", "2088122648957121").build()).build();

		System.out.println(query3.toJSON());
		List<QueryResult> result3 = tsdb.query(query3);
		System.out.println(result3);
		System.out.println("=========3=====");
//		
//		Query query4 = Query.timeRange(t0, t1).sub(SubQuery.metric("zhk.order.cnt", Aggregator.SUM)
//				.tag("supplierId", "2088122648957121").build()).build();
//
//		System.out.println(query4.toJSON());
//		List<QueryResult> result4 = tsdb.query(query4);
//		System.out.println(result4);
//		System.out.println("===========4===");
	}

	@Test
	public void testQueryCallback() {

		// int t0 = (int) (1508742134297l/1000);
		int t1 = (int) (1508742134297l / 1000);
		int t0 = t1 - 1;
		Query query = Query.timeRange(t0, t1).sub(SubQuery.metric("test-test-test").aggregator(Aggregator.AVG).tag("level", "500").build()).build();

		QueryCallback cb = new QueryCallback() {

			@Override
			public void response(String address, Query input, List<QueryResult> result) {
				System.out.println("查询参数：" + input);
				System.out.println("返回结果：" + result);
			}

			// 在需要处理异常的时候，重写failed方法
			@Override
			public void failed(String address, Query request, Exception ex) {
				super.failed(address, request, ex);
			}

		};

		tsdb.query(query, cb);

		try {
			Thread.sleep(100000000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
