package org.ptm.sidb.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.ptm.sidb.SiDBUtils;
import org.ptm.sidb.utils.SiDBResponse;
import org.ptm.sidb.utils.SiDB;

public class SiClientTest {
	
	SiDB sidb;

	@Before
	public void init() {
		sidb = SiDBUtils.pool("127.0.0.1", 5001, 2000, null);
		SiDBResponse resp = sidb.flushdb("");
		assertTrue(resp.ok());
	}
	
	@After
	public void depose() throws IOException {
		sidb.close();
	}
	
	@Test
	public void test_profiler() {
		sidb.set("abc", "1234");
		for (int i = 0; i < 10000; i++) {
			sidb.get("abc");
		}
	}
	
	@Test
	public void testSimpleClient() {
		assertNotNull(sidb);
	}


	@Test
	public void test_set_get_del() {
		SiDBResponse resp = sidb.set("name", "tommyphong");
		assertNotNull(resp);
		System.out.println(resp.stat);
		assertTrue(resp.ok());
		assertEquals(1, resp.datas.size());
		
		resp = sidb.get("name");
		assertNotNull(resp);
		assertTrue(resp.ok());
		assertEquals(1, resp.datas.size());
		assertEquals("tommyphong", new String(resp.datas.get(0)));
		
		resp = sidb.del("name");
		assertNotNull(resp);
		assertTrue(resp.ok());
		assertEquals(1, resp.datas.size());
		
		resp = sidb.get("name");
		assertNotNull(resp);
		assertFalse(resp.ok());
		assertTrue(resp.notFound());
	}

	@Test
	public void testIncr() {
		SiDBResponse resp = sidb.set("age", "28");
		assertNotNull(resp);
		assertTrue(resp.ok());

		sidb.incr("age", 1);
		sidb.incr("age", 2);
		sidb.incr("age", 3);
		
		resp = sidb.get("age");
		assertNotNull(resp);
		assertTrue(resp.ok());
		assertEquals(1, resp.datas.size());
		assertEquals(34, resp.asInt());
	}

	@Test
	public void testMulti_set_del() {
		SiDBResponse resp = sidb.multi_del("name", "age");
		sidb.del("name");
		sidb.del("age");
		resp = sidb.multi_set("name", "tommyphong_multi", "age", "18");
		assertNotNull(resp);
		assertTrue(resp.ok());
		
		resp = sidb.get("age");
		assertNotNull(resp);
		assertTrue(resp.ok());
		assertEquals(1, resp.datas.size());
		assertEquals("18", new String(resp.datas.get(0)));
		

		resp = sidb.get("name");
		assertNotNull(resp);
		assertTrue(resp.ok());
		assertEquals(1, resp.datas.size());
		assertEquals("tommyphong_multi", new String(resp.datas.get(0)));
		
		resp = sidb.multi_del("name", "age");
		assertNotNull(resp);
		assertTrue(resp.ok());
		
		resp = sidb.get("name");
		assertNotNull(resp);
		assertTrue(resp.notFound());
		resp = sidb.get("age");
		assertNotNull(resp);
		assertTrue(resp.notFound());
	}

	@Test
	public void testScan() {
		for (int i = 0; i < 1000; i++) {
			sidb.set("key" + i, i);
		}
		SiDBResponse resp = sidb.scan("", "", -1);
		assertTrue(resp.ok());
		Map<String, String> values = resp.toMapString();
		assertTrue(values.size() >= 1000);
		
		resp = sidb.scan("", "", 900);
		assertTrue(resp.ok());
		System.out.println(resp.toMapString().size());
		assertTrue(resp.toMapString().size() <= 900);
	}

	@Test
	public void test_batch() throws InterruptedException {
	    Thread.sleep(30*1000);
	    System.out.println(System.currentTimeMillis());
	    for (int i = 0; i < 1000; i++) {
            sidb.set("aaa" + i, i);
        }
	    System.out.println(System.currentTimeMillis());
		SiDB ssdb = this.sidb.batch();
		for (int i = 0; i < 1000; i++) {
			ssdb.set("aaa" + i, i);
		}
		System.out.println(System.currentTimeMillis());
		List<SiDBResponse> resps = ssdb.exec();
		System.out.println(System.currentTimeMillis());
		assertEquals(1000, resps.size());
		for (SiDBResponse resp : resps) {
			assertTrue(resp.ok());
		}
		Thread.sleep(30*1000);
	}

	@Test
	public void testHset() {
		sidb.del("my_map");
		sidb.hset("my_hash", "name", "tommyphong");
		sidb.hset("my_hash", "age", 27);
		
		SiDBResponse resp = sidb.hget("my_hash", "name");
		assertTrue(resp.ok());
		assertEquals("tommyphong", resp.asString());
		resp = sidb.hget("my_hash", "age");
		assertTrue(resp.ok());
		assertEquals(27, resp.asInt());
		
		sidb.hincr("my_hash", "age", 4);
		resp = sidb.hget("my_hash", "age");
		assertTrue(resp.ok());
		assertEquals(31, resp.asInt());
		

		resp = sidb.hsize("my_hash");
		assertTrue(resp.ok());
		assertEquals(2, resp.asInt());

		resp = sidb.hdel("my_hash", "name");
		assertTrue(resp.ok());
		resp = sidb.hdel("my_hash", "age");
		assertTrue(resp.ok());
		
		resp = sidb.hsize("my_hash");
		assertTrue(resp.ok());
		assertEquals(0, resp.asInt());
	}

	@Test
	public void test_info() {
		SiDBResponse resp = sidb.info();
		assertTrue(resp.ok());
		for (String str : resp.listString()) {
			System.out.println(str);
		}
	}

	@Test
	public void testZget() {
		sidb.zset("tommyphong", "net", 1);
		SiDBResponse resp = sidb.zget("tommyphong", "net");
		assertTrue(resp.ok());
		assertEquals(1, resp.asInt());
	}

	@Test
	public void testZdel() {
		sidb.zset("tommyphong", "net", 1);
		SiDBResponse resp = sidb.zdel("tommyphong", "net");
		assertTrue(resp.ok());
		resp = sidb.zget("tommyphong", "net");
		assertTrue(resp.notFound());
	}

	@Test
	public void testZincr() {
		sidb.zset("tommyphong", "net", 1);
		SiDBResponse resp = sidb.zincr("tommyphong", "net", 10);
		assertTrue(resp.ok());
		assertEquals(11, resp.asInt());
	}

	@Test
	public void testZsize() {
		sidb.zset("tommyphong", "net", 1);
		sidb.zset("tommyphong", "net2", 1);
		sidb.zset("tommyphong", "ne3", 1);
		sidb.zset("tommyphong", "ne8", 1);
		SiDBResponse resp = sidb.zsize("tommyphong");
		assertTrue(resp.ok());
		assertEquals(4, resp.asInt());
	}

	@Test
	public void testZrank() {
		for (int i = 0; i < 100; i++) {
			sidb.zset("tommyphong", "net-"+i, i + 100);
		}
		SiDBResponse resp = sidb.zsize("tommyphong");
		assertTrue(resp.ok());
		assertEquals(100, resp.asInt());
		resp = sidb.zrank("tommyphong", "net-33");
		assertTrue(resp.ok());
		assertEquals(33, resp.asInt());
	}

	@Test
	public void testZrrank() {
		for (int i = 0; i < 100; i++) {
			sidb.zset("tommyphong", "net-"+i, i + 100);
		}
		SiDBResponse resp = sidb.zsize("tommyphong");
		assertTrue(resp.ok());
		assertEquals(100, resp.asInt());
		resp = sidb.zrrank("tommyphong", "net-33");
		assertTrue(resp.ok());
		assertEquals(100 - 33 - 1, resp.asInt());
	}

	@Test
	public void testZrange() {
		for (int i = 0; i < 100; i++) {
			sidb.zset("tommyphong", "net-"+i, i + 100);
		}
		SiDBResponse resp = sidb.zsize("tommyphong");
		assertTrue(resp.ok());
		assertEquals(100, resp.asInt());
		resp = sidb.zrange("tommyphong", 20, 10);
		assertTrue(resp.ok());
		assertEquals(10, resp.toMap().size());
	}

	@Test
	public void testZrrange() {
		for (int i = 0; i < 100; i++) {
			sidb.zset("tommyphong", "net-"+i, i + 100);
		}
		SiDBResponse resp = sidb.zsize("tommyphong");
		assertTrue(resp.ok());
		assertEquals(100, resp.asInt());
		resp = sidb.zrrange("tommyphong", 20, 10);
		assertTrue(resp.ok());
		assertEquals(10, resp.toMap().size());
	}

	@Test
	public void testZscan() {
		sidb.zset("tommyphong", "net", 1);
		sidb.zset("tommyphong", "net2", 3);
		sidb.zset("tommyphong", "net3", 4);
		SiDBResponse resp = sidb.zscan("tommyphong", "", 1, 2, 2);
		assertTrue(resp.ok());
		assertEquals(1, resp.toMap().size());
	}

	@Test
	public void testZrscan() {
		sidb.zset("tommyphong", "net", 1);
		sidb.zset("tommyphong", "net2", 3);
		sidb.zset("tommyphong", "net3", 4);
		SiDBResponse resp = sidb.zrscan("tommyphong", "", 7, 1, 2);
		assertTrue(resp.ok());
		assertEquals(2, resp.toMap().size());
	}

	@Test
	public void testQsize() {
		SiDBResponse resp = sidb.qpush("qtommyphong", 1);
		assertTrue(resp.ok());
		assertEquals(1, resp.asInt());
		resp = sidb.qsize("qtommyphong");
		assertTrue(resp.ok());
		assertEquals(1, resp.asInt());
	}
	

	@Test
	public void testQpush() {
		SiDBResponse resp = sidb.qpush("q1", 123);
		assertTrue(resp.ok());
		assertEquals(1, resp.asInt());

		assertEquals(123, sidb.qpop("q1").asInt());
		assertEquals(4, sidb.qpop("q1").asInt());
		assertEquals(7, sidb.qpop("q1").asInt());
		assertEquals(2, sidb.qpop("q1").asInt());
		assertEquals(1, sidb.qpop("q1").asInt());
	}

	@Test
	public void testSetnx() {
		sidb.set("abc", "1");
		SiDBResponse resp = sidb.setnx("abc", "2");
		assertTrue(resp.ok());
		assertEquals(0, resp.asInt());
		
		resp = sidb.setnx("abc2", "2");
		assertTrue(resp.ok());
		assertEquals(1, resp.asInt());
		System.out.println((char)85);
	}
	
}
