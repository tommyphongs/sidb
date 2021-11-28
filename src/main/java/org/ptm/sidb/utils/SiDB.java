package org.ptm.sidb.utils;

import java.io.Closeable;
import java.util.List;

public interface SiDB extends Closeable {

	SiDBResponse get(Object key);

	SiDBResponse set(Object key, Object val);

	SiDBResponse setx(Object key, Object val, int ttl);

	SiDBResponse del(Object key);

	SiDBResponse incr(Object key, int val);

	SiDBResponse decr(Object key, int val);

	SiDBResponse exists(Object key);

	SiDBResponse multi_exists(Object... keys);

	SiDBResponse keys(Object start, Object end, int limit);

	SiDBResponse multi_set(Object... pairs);

	SiDBResponse multi_get(Object... keys);

	SiDBResponse multi_del(Object... keys);

	SiDBResponse scan(Object start, Object end, int limit);

	SiDBResponse rscan(Object start, Object end, int limit);

	SiDBResponse hset(Object key, Object hkey, Object hval);

	SiDBResponse hdel(Object key, Object hkey);

	SiDBResponse hget(Object key, Object hkey);

	SiDBResponse hsize(Object key);

	SiDBResponse hlist(Object key, Object hkey, int limit);

	SiDBResponse hincr(Object key, Object hkey, int val);

	SiDBResponse hdecr(Object key, Object hkey, int val);

	SiDBResponse hscan(Object key, Object start, Object end, int limit);

	SiDBResponse hrscan(Object key, Object start, Object end, int limit);

	SiDBResponse hkeys(Object key, Object start, Object end, int limit);

	SiDBResponse hexists(Object key, Object hkey);

	SiDBResponse hclear(Object key);

	SiDBResponse hgetall(Object key);

	SiDBResponse hvals(Object key, Object start, Object end, int limit);

	SiDBResponse multi_hget(Object key, Object... hkeys);

	SiDBResponse multi_hset(Object key, Object... pairs);

	SiDBResponse multi_hdel(Object key, Object... hkeys);

	SiDBResponse multi_hexists(Object... keys);

	SiDBResponse multi_hsize(Object... keys);

	SiDBResponse zset(Object key, Object zkey, long score);

	SiDBResponse zget(Object key, Object zkey);

	SiDBResponse zdel(Object key, Object zkey);

	SiDBResponse zincr(Object key, Object zkey, int val);

	SiDBResponse zdecr(Object key, Object zkey, int val);

	SiDBResponse zlist(Object key_start, Object key_end, int limit);

	SiDBResponse zsize(Object key);

	SiDBResponse zrank(Object key, Object zkey);

	SiDBResponse zrrank(Object key, Object zkey);

	SiDBResponse zexists(Object key, Object zkey);

	SiDBResponse zclear(Object key);

	SiDBResponse zremrangebyrank(Object key, Object score_start, Object score_end);

	SiDBResponse zremrangebyscore(Object key, Object score_start, Object score_end);

	SiDBResponse zkeys(Object key, Object zkey_start, Object score_start, Object score_end, int limit);

	SiDBResponse zscan(Object key, Object zkey_start, Object score_start, Object score_end, int limit);

	SiDBResponse zrscan(Object key, Object zkey_start, Object score_start, Object score_end, int limit);

	SiDBResponse zrange(Object key, int offset, int limit);

	SiDBResponse zrrange(Object key, int offset, int limit);

	SiDBResponse multi_zset(Object key, Object... pairs);

	SiDBResponse multi_zget(Object key, Object... zkeys);

	SiDBResponse multi_zdel(Object key, Object... zkeys);

	SiDBResponse multi_zexists(Object key, Object... zkeys);

	SiDBResponse multi_zsize(Object... keys);

	SiDBResponse qsize(Object key);

	SiDBResponse qfront(Object key);

	SiDBResponse qback(Object key);

	SiDBResponse qpush(Object key, Object value);

	SiDBResponse qpush(Object key, Object... value);

	SiDBResponse qpush_front(Object key, Object value);

	SiDBResponse qpush_back(Object key, Object value);

	SiDBResponse qpop(Object key);

	SiDBResponse qpop(Object key, int limit);

	SiDBResponse qpop_front(Object key);

	SiDBResponse qpop_back(Object key);

	SiDBResponse qfix(Object key);

	SiDBResponse qlist(Object key_start, Object key_end, int limit);

	SiDBResponse qclear(Object key);

	SiDBResponse qrange(Object key, int begin, int limit);

	SiDBResponse flushdb(String type);

	SiDBResponse info();

	SiDBResponse ping();

	SiDB batch();

	List<SiDBResponse> exec();

	SiDBResponse setnx(Object key, Object val);

	SiDBResponse getset(Object key, Object val);

	SiDBResponse qslice(Object key, int start, int end);

	SiDBResponse qget(Object key, int index);

	SiDBResponse zcount(Object key, int start, int end);

	SiDBResponse zsum(Object key, int start, int end);

	SiDBResponse zavg(Object key, int start, int end);

	SiDBResponse eval(Object lua, Object... args);

	SiDBResponse evalsha(Object sha1, Object... args);

	SiDBResponse ttl(Object key);

	SiDBResponse expire(Object key, int ttl);

	SiDBResponse key_range();

	SiDBResponse compact();


	SiDBResponse getbit(Object key, int offset);

	SiDBResponse setbit(Object key, int offset, byte on);

	SiDBResponse countbit(Object key, int start, int size);

	SiDBResponse substr(Object key, int start, int size);

	SiDBResponse getrange(Object key, int start, int size);

	SiDBResponse strlen(Object key);

	SiDBResponse redis_bitcount(Object key, int start, int size);

	SiDBResponse hrlist(Object key, Object hkey, int limit);

	SiDBResponse zrlist(Object zkey_start, Object zkey_end, int limit);

	SiDBResponse qrlist(Object key_start, Object key_end, int limit);


	SiDBResponse auth(String passwd);

	SiDBResponse qtrim_front(Object key, int size);

	SiDBResponse qtrim_back(Object key, int size);

	void changeObjectConv(DataModeling.DataBlock conv);

	SiDBResponse req(Runner cmd, byte[]... values);

}
