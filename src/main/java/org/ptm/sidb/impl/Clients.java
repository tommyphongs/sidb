package org.ptm.sidb.impl;

import org.ptm.sidb.SiDBUtils;
import org.ptm.sidb.utils.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class Clients {
    public static class SiDBClient implements SiDB {

        protected Streams.SiDBStream stream;

        protected DataModeling.DataBlock conv;

        public SiDBClient(Streams.SiDBStream stream) {
            this.stream = stream;
            this.conv = DataModeling.DefaultDataBlock.me;
        }

        public SiDBClient(String host, int port, int timeout) {
            stream = new Streams.SocketSiDBStream(host, port, timeout);
            this.conv = DataModeling.DefaultDataBlock.me;
        }

        protected byte[] bytes(Object obj) {
            return conv.toBytes(obj);
        }

        protected byte[][] bytess(Object... objs) {
            return conv.toBytes(objs);
        }


        protected SiDBResponse req(Runner cmd, byte[] first, byte[][] lots) {
            byte[][] vals = new byte[lots.length+1][];
            vals[0] = first;
            for (int i = 0; i < lots.length; i++) {
                vals[i+1] = lots[i];
            }
            return req(cmd, vals);
        }

        public SiDBResponse req(Runner cmd, byte[] ... vals) {
            return stream.request(cmd, vals);
        }

        public SiDB batch() {
            return new SiDBClientBatch(stream, 60, TimeUnit.SECONDS);
        }

        public SiDB batch(int timeout, TimeUnit timeUnit) {
            return new SiDBClientBatch(stream, timeout, timeUnit);
        }

        public List<SiDBResponse> exec() {
            throw new SiDBException("not batch!");
        }

        public void setObjectConv(DataModeling.DataBlock conv) {
            this.conv = conv;
        }

        public void changeObjectConv(DataModeling.DataBlock conv) {
            this.setObjectConv(conv);
        }

        public void setSiDBStream(Streams.SiDBStream stream) {
            this.stream = stream;
        }

        //----------------------------------------------------------------------------------

        public SiDBResponse get(Object key) {
            return req(Runner.get,bytes(key));
        }


        public SiDBResponse set(Object key, Object val) {
            return req(Runner.set,bytes(key), bytes(val));
        }


        public SiDBResponse setx(Object key, Object val, int ttl) {
            return req(Runner.setx,bytes(key), bytes(val), Integer.toString(ttl).getBytes());
        }


        public SiDBResponse del(Object key) {
            return req(Runner.del,bytes(key));
        }


        public SiDBResponse incr(Object key, int val) {
            return req(Runner.incr,bytes(key), Integer.toString(val).getBytes());
        }


        public SiDBResponse exists(Object key) {
            return req(Runner.exists,bytes(key));
        }


        public SiDBResponse keys(Object start, Object end, int limit) {
            return req(Runner.keys,bytes(start), bytes(end), Integer.toString(limit).getBytes());
        }


        public SiDBResponse multi_set(Object... pairs) {
            return req(Runner.multi_set,bytess(pairs));
        }


        public SiDBResponse multi_get(Object... keys) {
            return req(Runner.multi_get,bytess(keys));
        }


        public SiDBResponse multi_del(Object... keys) {
            return req(Runner.multi_del,bytess(keys));
        }


        public SiDBResponse scan(Object start, Object end, int limit) {
            return req(Runner.scan,bytes(start), bytes(end), Integer.toString(limit).getBytes());
        }


        public SiDBResponse rscan(Object start, Object end, int limit) {
            return req(Runner.rscan,bytes(start), bytes(end), Integer.toString(limit).getBytes());
        }


        public SiDBResponse hset(Object key, Object hkey, Object hval) {
            return req(Runner.hset,bytes(key), bytes(hkey), bytes(hval));
        }


        public SiDBResponse hdel(Object key, Object hkey) {
            return req(Runner.hdel,bytes(key), bytes(hkey));
        }


        public SiDBResponse hget(Object key, Object hkey) {
            return req(Runner.hget,bytes(key), bytes(hkey));
        }


        public SiDBResponse hsize(Object key) {
            return req(Runner.hsize,bytes(key));
        }


        public SiDBResponse hlist(Object key, Object hkey, int limit) {
            return req(Runner.hlist,bytes(key), bytes(hkey), Integer.toString(limit).getBytes());
        }


        public SiDBResponse hincr(Object key, Object hkey, int val) {
            return req(Runner.hincr,bytes(key), bytes(hkey), Integer.toString(val).getBytes());
        }


        public SiDBResponse hscan(Object key, Object start, Object end, int limit) {
            return req(Runner.hscan,bytes(key), bytes(start), bytes(end), Integer.toString(limit).getBytes());
        }


        public SiDBResponse hrscan(Object key, Object start, Object end, int limit) {
            return req(Runner.hrscan,bytes(key), bytes(start), bytes(end), Integer.toString(limit).getBytes());
        }

        public SiDBResponse zset(Object key, Object zkey, long score) {
            return req(Runner.zset,bytes(key), bytes(zkey), Long.toString(score).getBytes());
        }


        public SiDBResponse zget(Object key, Object zkey) {
            return req(Runner.zget,bytes(key), bytes(zkey));
        }


        public SiDBResponse zdel(Object key, Object zkey) {
            return req(Runner.zdel,bytes(key), bytes(zkey));
        }


        public SiDBResponse zincr(Object key, Object zkey, int val) {
            return req(Runner.zincr,bytes(key), bytes(zkey), Integer.toString(val).getBytes());
        }


        public SiDBResponse zsize(Object key) {
            return req(Runner.zsize,bytes(key));
        }


        public SiDBResponse zlist(Object zkey_start, Object zkey_end, int limit) {
            return req(Runner.zlist, bytes(zkey_start), bytes(zkey_end), Integer.toString(limit).getBytes());
        }


        public SiDBResponse zrank(Object key, Object zkey) {
            return req(Runner.zrank,bytes(key), bytes(zkey));
        }


        public SiDBResponse zrrank(Object key, Object zkey) {
            return req(Runner.zrrank, bytes(key), bytes(zkey));
        }


        public SiDBResponse zscan(Object key, Object zkey_start, Object score_start, Object score_end, int limit) {
            return req(Runner.zscan, bytes(key), bytes(zkey_start), bytes(score_start), bytes(score_end), Integer.toString(limit).getBytes());
        }


        public SiDBResponse zrscan(Object key, Object zkey_start, Object score_start, Object score_end, int limit) {
            return req(Runner.zrscan, bytes(key), bytes(zkey_start), bytes(score_start), bytes(score_end), Integer.toString(limit).getBytes());
        }


        public SiDBResponse qsize(Object key) {
            return req(Runner.qsize, bytes(key));
        }


        public SiDBResponse qfront(Object key) {
            return req(Runner.qfront, bytes(key));
        }


        public SiDBResponse qback(Object key) {
            return req(Runner.qback, bytes(key));
        }


        public SiDBResponse qpush(Object key, Object value) {
            return req(Runner.qpush, bytes(key), bytes(value));
        }

        public SiDBResponse qpush(Object key, Object... value) {
            return req(Runner.qpush, bytes(key), bytess(value));
        }


        public SiDBResponse qpop(Object key) {
            return req(Runner.qpop, bytes(key));
        }

        public SiDBResponse qpop(Object key, int limit) {
            return req(Runner.qpop, bytes(key), Integer.toString(limit).getBytes());
        }

        public SiDBResponse qlist(Object key_start, Object key_end, int limit) {
            return req(Runner.qlist, bytes(key_start), bytes(key_end), Integer.toString(limit).getBytes());
        }


        public SiDBResponse qclear(Object key) {
            return req(Runner.qclear, bytes(key));
        }


        public SiDBResponse hkeys(Object key, Object start, Object end, int limit) {
            return req(Runner.hkeys, bytes(key), bytes(start), bytes(end), Integer.toString(limit).getBytes());
        }


        public SiDBResponse hexists(Object key, Object hkey) {
            return req(Runner.hexists, bytes(key), bytes(hkey));
        }


        public SiDBResponse hclear(Object key) {
            return req(Runner.hclear, bytes(key));
        }


        public SiDBResponse multi_hget(Object key, Object... hkeys) {
            return req(Runner.multi_hget, bytes(key), bytess(hkeys));
        }


        public SiDBResponse multi_hset(Object key, Object... pairs) {
            return req(Runner.multi_hset, bytes(key), bytess(pairs));
        }


        public SiDBResponse multi_hdel(Object key, Object... hkeys) {
            return req(Runner.multi_hdel, bytes(key), bytess(hkeys));
        }


        public SiDBResponse zexists(Object key, Object zkey) {
            return req(Runner.zexists, bytes(key), bytes(zkey));
        }


        public SiDBResponse zclear(Object key) {
            return req(Runner.zclear, bytes(key));
        }


        public SiDBResponse zkeys(Object key, Object zkey_start, Object score_start, Object score_end, int limit) {
            return req(Runner.zkeys, bytes(key), bytes(zkey_start), bytes(score_start), bytes(score_end), Integer.toString(limit).getBytes());
        }


        public SiDBResponse zrange(Object key, int offset, int limit) {
            return req(Runner.zrange, bytes(key), Integer.toString(offset).getBytes(), Integer.toString(limit).getBytes());
        }


        public SiDBResponse zrrange(Object key, int offset, int limit) {
            return req(Runner.zrrange, bytes(key), Integer.toString(offset).getBytes(), Integer.toString(limit).getBytes());
        }


        public SiDBResponse multi_zset(Object key, Object... pairs) {
            return req(Runner.multi_zset, bytes(key), bytess(pairs));
        }


        public SiDBResponse multi_zget(Object key, Object... zkeys) {
            return req(Runner.multi_zget, bytes(key), bytess(zkeys));
        }


        public SiDBResponse multi_zdel(Object key, Object... zkeys) {
            return req(Runner.multi_zdel, bytes(key), bytess(zkeys));
        }


        public SiDBResponse flushdb(String type) {
            if (type == null || type.length() == 0) {
                flushdb_kv();
                flushdb_hash();
                flushdb_zset();
                flushdb_list();
            } else if ("kv".equals(type)) {
                flushdb_kv();
            } else if ("hash".equals(type)) {
                flushdb_hash();
            } else if ("zset".equals(type)) {
                flushdb_zset();
            } else if ("list".equals(type)) {
                flushdb_list();
            } else {
                throw new IllegalArgumentException("not such flushdb mode=" + type);
            }
            SiDBResponse resp = new SiDBResponse();
            resp.stat = "ok";
            return resp;
        }

        protected long flushdb_kv() {
            long count = 0;
            while (true) {
                List<String> keys = keys("", "", 1000).validate().listString();
                if (keys.isEmpty())
                    return count;
                count += keys.size();
                multi_del(keys.toArray());
            }
        }

        protected long flushdb_hash() {
            long count = 0;
            while (true) {
                List<String> keys = hlist("", "", 1000).validate().listString();
                if (keys.isEmpty())
                    return count;
                count += keys.size();
                for (String key : keys) {
                    hclear(key);
                }
            }
        }

        protected long flushdb_zset() {
            long count = 0;
            while (true) {
                List<String> keys = zlist("", "", 1000).validate().listString();
                if (keys.isEmpty())
                    return count;
                count += keys.size();
                for (String key : keys) {
                    zclear(key);
                }
            }
        }

        protected long flushdb_list() {
            long count = 0;
            while (true) {
                List<String> keys = qlist("", "", 1000).validate().listString();
                if (keys.isEmpty())
                    return count;
                count += keys.size();
                for (String key : keys) {
                    qclear(key);
                }
            }
        }


        public SiDBResponse info() {
            return req(Runner.info);
        }


        public SiDBResponse ping() {
            return req(Runner.ping);
        }

        //------------------------------------------


        public SiDBResponse setnx(Object key, Object val) {
            return req(Runner.setnx, bytes(key), bytes(val));
        }


        public SiDBResponse getset(Object key, Object val) {
            return req(Runner.getset, bytes(key), bytes(val));
        }


        public SiDBResponse qslice(Object key, int start, int end) {
            return req(Runner.qslice, bytes(key), Integer.toString(start).getBytes(), Integer.toString(end).getBytes());
        }


        public SiDBResponse qget(Object key, int index) {
            return req(Runner.qget, bytes(key), Integer.toString(index).getBytes());
        }


        public SiDBResponse zcount(Object key, int start, int end) {
            return req(Runner.zcount, bytes(key), Integer.toString(start).getBytes(), Integer.toString(end).getBytes());
        }


        public SiDBResponse zsum(Object key, int start, int end) {
            return req(Runner.zsum, bytes(key), Integer.toString(start).getBytes(), Integer.toString(end).getBytes());
        }


        public SiDBResponse zavg(Object key, int start, int end) {
            return req(Runner.zavg, bytes(key), Integer.toString(start).getBytes(), Integer.toString(end).getBytes());
        }


        public SiDBResponse eval(Object key, Object... args) {
            return req(Runner.eval, bytes(key), bytess(args));
        }


        public SiDBResponse evalsha(Object sha1, Object... args) {
            return req(Runner.evalsha, bytes(sha1), bytess(args));
        }


        public SiDBResponse ttl(Object key) {
            return req(Runner.ttl, bytes(key));
        }


        public SiDBResponse decr(Object key, int val) {
            return req(Runner.decr, bytes(key), Integer.toString(val).getBytes());
        }


        public SiDBResponse multi_exists(Object... keys) {
            return req(Runner.multi_exists, bytess(keys));
        }


        public SiDBResponse hdecr(Object key, Object hkey, int val) {
            return req(Runner.hdecr, bytes(key), bytes(hkey), Integer.toString(val).getBytes());
        }


        public SiDBResponse hgetall(Object key) {
            return req(Runner.hgetall, bytes(key));
        }


        public SiDBResponse hvals(Object key, Object start, Object end, int limit) {
            return req(Runner.hvals, bytes(key), bytes(start), bytes(end), Integer.toString(limit).getBytes());
        }


        public SiDBResponse multi_hexists(Object... keys) {
            return req(Runner.hvals, bytess(keys));
        }


        public SiDBResponse multi_hsize(Object... keys) {
            return req(Runner.multi_hsize, bytes(keys));
        }


        public SiDBResponse zdecr(Object key, Object zkey, int val) {
            return req(Runner.zdecr, bytes(key), bytes(zkey), Integer.toString(val).getBytes());
        }


        public SiDBResponse zremrangebyrank(Object key, Object score_start, Object score_end) {
            return req(Runner.zremrangebyrank, bytes(key), bytes(score_start), bytes(score_end));
        }


        public SiDBResponse zremrangebyscore(Object key, Object score_start, Object score_end) {
            return req(Runner.zremrangebyscore, bytes(key), bytes(score_start), bytes(score_end));
        }


        public SiDBResponse multi_zexists(Object key, Object... zkeys) {
            return req(Runner.zexists, bytes(key), bytess(zkeys));
        }


        public SiDBResponse multi_zsize(Object... keys) {
            return req(Runner.zsize, bytess(keys));
        }


        public SiDBResponse qpush_back(Object key, Object value) {
            return req(Runner.qpush_back, bytes(key), bytes(value));
        }


        public SiDBResponse qpush_front(Object key, Object value) {
            return req(Runner.qpush_front, bytes(key), bytes(value));
        }


        public SiDBResponse qpop_back(Object key) {
            return req(Runner.qpop_back, bytes(key));
        }


        public SiDBResponse qpop_front(Object key) {
            return req(Runner.qpop_front, bytes(key));
        }


        public SiDBResponse qrange(Object key, int begin, int limit) {
            return req(Runner.qrange, bytes(key), Integer.toString(begin).getBytes(), Integer.toString(limit).getBytes());
        }


        public SiDBResponse qfix(Object key) {
            return req(Runner.qfix, bytes(key));
        }


        public SiDBResponse dump() {
            return req(Runner.dump);
        }


        public SiDBResponse clear_binlog() {
            return req(Runner.clear_binlog);
        }


        public SiDBResponse compact() {
            return req(Runner.compact);
        }


        public SiDBResponse expire(Object key, int ttl) {
            return req(Runner.expire, bytes(key), Integer.toString(ttl).getBytes());
        }


        public SiDBResponse key_range() {
            return req(Runner.key_range);
        }


        public SiDBResponse sync140() {
            return null;
        }


        public void close() throws IOException {
            stream.close();
        }

        public SiDBResponse getbit(Object key, int offset) {
            return req(Runner.getbit, bytes(key), Integer.toString(offset).getBytes());
        }

        public SiDBResponse setbit(Object key, int offset, byte on) {
            return req(Runner.setbit, bytes(key), Integer.toString(offset).getBytes(), on == 1 ? "1".getBytes() : "0".getBytes());
        }

        public SiDBResponse countbit(Object key, int start, int size) {
            return req(Runner.countbit, bytes(key), Integer.toString(start).getBytes(), Integer.toString(size).getBytes());
        }

        public SiDBResponse substr(Object key, int start, int size) {
            if (size < 0)
                size = 2000000000;
            return req(Runner.strlen, bytes(key), Integer.toString(start).getBytes(), Integer.toString(size).getBytes());
        }

        public SiDBResponse getrange(Object key, int start, int size) {
            return req(Runner.getrange, bytes(key), Integer.toString(start).getBytes(), Integer.toString(size).getBytes());
        }

        public SiDBResponse strlen(Object key) {
            return req(Runner.strlen, bytes(key));
        }

        public SiDBResponse redis_bitcount(Object key, int start, int size) {
            return req(Runner.redis_bitcount, bytes(key), Integer.toString(start).getBytes(), Integer.toString(size).getBytes());
        }

        public SiDBResponse hrlist(Object key, Object hkey, int limit) {
            return req(Runner.hrlist,bytes(key), bytes(hkey), Integer.toString(limit).getBytes());
        }

        public SiDBResponse zrlist(Object zkey_start, Object zkey_end, int limit) {
            return req(Runner.zrlist, bytes(zkey_start), bytes(zkey_end), Integer.toString(limit).getBytes());
        }

        public SiDBResponse qrlist(Object key_start, Object key_end, int limit) {
            return req(Runner.qrlist, bytes(key_start), bytes(key_end), Integer.toString(limit).getBytes());
        }

        public SiDBResponse auth(String passwd) {
            return req(Runner.auth, bytes(passwd));
        }

        public SiDBResponse qtrim_back(Object key, int size) {
            return req(Runner.qtrim_back, bytes(key), Integer.toString(size).getBytes());
        }

        public SiDBResponse qtrim_front(Object key, int size) {
            return req(Runner.qtrim_front, bytes(key), Integer.toString(size).getBytes());
        }
    }

    public static class SiDBClientBatch extends SiDBClient implements SiDBStreamCallback {

        protected static SiDBResponse OK = new SiDBResponse();
        static {
            OK.stat = "ok";
        }

        protected List<_Req> reqs;

        protected Object respLock = new Object();

        protected int count;

        protected List<SiDBResponse> resps;

        protected int timeout;

        protected TimeUnit timeUnit;

        public SiDBClientBatch(Streams.SiDBStream stream, int timeout, TimeUnit timeUnit) {
            super(stream);
            if (timeout < 0 || timeUnit == null)
                throw new IllegalArgumentException("timeout must bigger than 0, and timeUnit must not null");
            this.timeout = timeout;
            this.timeUnit = timeUnit;
            this.resps = new ArrayList<SiDBResponse>();
            this.reqs = new ArrayList<_Req>();
        }

        public SiDBResponse req(Runner cmd, byte[]... vals) {
            if (reqs == null)
                throw new SiDBException("this BatchClient is invaild!");
            reqs.add(new _Req(cmd, vals));
            return OK;
        }

        public synchronized List<SiDBResponse> exec() {
            if (reqs == null)
                throw new SiDBException("this BatchClient is invaild!");
            count = reqs.size();
            stream.callback(this);
            List<SiDBResponse> resps = this.resps;
            this.resps = null;
            return resps;
        }

        public void involve(final InputStream in, final OutputStream out) {
            ExecutorService es = Executors.newFixedThreadPool(2);
            es.submit(new Callable<Object>() {
                public Object call() throws Exception {
                    for (int i = 0; i < count; i++) {
                        resps.add(SiDBUtils.readResp(in));
                    }
                    return null;
                }
            });
            es.submit(new Callable<Object>() {
                public Object call() throws Exception {
                    for (_Req req : reqs) {
                        SiDBUtils.writeBlock(out, req.cmd.bytes());
                        for (byte[] bs : req.vals) {
                            SiDBUtils.writeBlock(out, bs);
                        }
                        out.write('\n');
                    }
                    out.flush();
                    return null;
                }
            });
            try {
                es.shutdown();
                boolean flag = es.awaitTermination(timeout, timeUnit);
                reqs = null;
                if (!flag)
                    throw new RuntimeException(new TimeoutException("batch execute timeout!"));
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            finally {
                es.shutdownNow();
            }
        }

        public SiDB batch() {
            throw new SiDBException("aready in batch mode, not support for batch again");
        }

        static class _Req {
            public Runner cmd;
            public byte[][] vals;

            public _Req(Runner cmd, byte[]... vals) {
                this.cmd = cmd;
                this.vals = vals;
            }
        }
    }
}
