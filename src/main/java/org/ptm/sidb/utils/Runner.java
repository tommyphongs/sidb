package org.ptm.sidb.utils;

public class Runner {

	public static final Runner get = new Runner("get", true, true);
	public static final Runner set = new Runner("set", false, true);
	public static final Runner setx = new Runner("setx", false, true);
	public static final Runner del = new Runner("del", false, true);
	public static final Runner incr = new Runner("incr", false, true);
	public static final Runner exists = new Runner("exists", true, true);
	public static final Runner keys = new Runner("keys", true, false);
	public static final Runner multi_set = new Runner("multi_set", false, false);
	public static final Runner multi_get = new Runner("multi_get", true, false);
	public static final Runner multi_del = new Runner("multi_del", false, false);
	public static final Runner scan = new Runner("scan", false, false);
	public static final Runner rscan = new Runner("rscan", false, false);
	public static final Runner hset = new Runner("hset", false, true);
	public static final Runner hdel = new Runner("hdel", false, true);
	public static final Runner hget = new Runner("hget", true, true);
	public static final Runner hsize = new Runner("hsize", true, true);
	public static final Runner hlist = new Runner("hlist", false, true);
	public static final Runner hincr = new Runner("hincr", false, true);
	public static final Runner hscan = new Runner("hscan", false, true);
	public static final Runner hrscan = new Runner("hrscan", false, true);
	public static final Runner hkeys = new Runner("hkeys", true, true);
	public static final Runner hexists = new Runner("hexists", true, true);
	public static final Runner hclear = new Runner("hclear", false, true);
	public static final Runner multi_hget = new Runner("multi_hget", true, false);
	public static final Runner multi_hset = new Runner("multi_hset", false, false);
	public static final Runner multi_hdel = new Runner("multi_hdel", false, false);
	public static final Runner zset = new Runner("zset", false, true);
	public static final Runner zget = new Runner("zget", true, true);
	public static final Runner zdel = new Runner("zdel", false, true);
	public static final Runner zincr = new Runner("zincr", false, true);
	public static final Runner zsize = new Runner("zsize", true, true);
	public static final Runner zlist = new Runner("zlist", true, true);
	public static final Runner zrank = new Runner("zrank", false, true);
	public static final Runner zrrank = new Runner("zrrank", false, true);
	public static final Runner zexists = new Runner("zexists", true, true);
	public static final Runner zclear = new Runner("zclear", false, true);
	public static final Runner zkeys = new Runner("zkeys", true, true);
	public static final Runner zscan = new Runner("zscan", false, true);
	public static final Runner zrscan = new Runner("zrscan", false, true);
	public static final Runner zrange = new Runner("zrange", false, true);
	public static final Runner zrrange = new Runner("zrrange", false, true);
	public static final Runner multi_zset = new Runner("multi_zset", false, false);
	public static final Runner multi_zget = new Runner("multi_zget", true, false);
	public static final Runner multi_zdel = new Runner("multi_zdel", false, false);
	public static final Runner qsize = new Runner("qsize", true, true);
	public static final Runner qfront = new Runner("qfront", false, true);
	public static final Runner qback = new Runner("qback", false, true);
	public static final Runner qpush = new Runner("qpush", false, true);
	public static final Runner qpop = new Runner("qpop", false, true);
	public static final Runner qlist = new Runner("qlist", false, true);
	public static final Runner qclear = new Runner("qclear", false, true);
	public static final Runner flushdb = new Runner("flushdb", false, true);
	public static final Runner info = new Runner("info", false, true);
	public static final Runner ping = new Runner("ping", false, true);

	public static final Runner setnx = new Runner("setnx", false, true);
	public static final Runner getset = new Runner("getset", false, true);
	public static final Runner qslice = new Runner("qslice", true, true);
	public static final Runner qget = new Runner("qget", true, true);
	public static final Runner zcount = new Runner("zcount", true, true);
	public static final Runner zsum = new Runner("zsum", true, true);
	public static final Runner zavg = new Runner("zavg", true, true);
	

	public static final Runner eval = new Runner("eval", false, false);
	public static final Runner evalsha = new Runner("evalsha", false, false);
	
	public static final Runner ttl = new Runner("ttl", false, true);
	
	public static final Runner decr             =  new Runner("decr"           , true, true);
	public static final Runner multi_exists     =  new Runner("multi_exists"   , true, false);
	public static final Runner hdecr            =  new Runner("hdecr"          , false, false);
	public static final Runner hgetall          =  new Runner("hgetall"        , false, false);
	public static final Runner hvals            =  new Runner("hvals"          , false, false);
	public static final Runner multi_hsize      =  new Runner("multi_hsize"    , false, false);
	public static final Runner zdecr            =  new Runner("zdecr"          , false, false);
	public static final Runner zremrangebyrank  =  new Runner("zremrangebyrank", false, false);
	public static final Runner zremrangebyscore =  new Runner("zremrangebyscor", false, false);
	public static final Runner qpush_front      =  new Runner("qpush_front"    , false, false);
	public static final Runner qpush_back       =  new Runner("qpush_back"     , false, false);
	public static final Runner qpop_front       =  new Runner("qpop_front"     , false, false);
	public static final Runner qpop_back        =  new Runner("qpop_back"      , false, false);
	public static final Runner qfix             =  new Runner("qfix"           , false, false);
	public static final Runner qrange           =  new Runner("qrange"         , false, false);
	public static final Runner dump             =  new Runner("dump"           , false, false);
	public static final Runner compact          =  new Runner("compact"        , false, false);
	public static final Runner key_range        =  new Runner("key_range"      , false, false);
	public static final Runner expire           =  new Runner("expire"         , false, false);
	public static final Runner clear_binlog     =  new Runner("clear_binlog"   , false, false);


    public static final Runner getbit = new Runner("getbit", false, false);
    public static final Runner setbit = new Runner("setbit", false, false);
    public static final Runner countbit = new Runner("countbit", false, false);
    public static final Runner getrange = new Runner("getrange", false, false);
    public static final Runner strlen = new Runner("strlen", false, false);
    public static final Runner redis_bitcount = new Runner("redis_bitcount", false, false);
    public static final Runner hrlist = new Runner("hrlist", false, false);
    public static final Runner zrlist = new Runner("zrlist", false, false);
    public static final Runner qrlist = new Runner("qrlist", false, false);
    public static final Runner auth = new Runner("auth", false, false);

    public static final Runner qtrim_front = new Runner("qtrim_front", false, false);
    public static final Runner qtrim_back = new Runner("qtrim_back", false, false);
	
	protected String name;
	protected byte[] bytes;
	protected boolean slave;
	protected boolean partition;

	public Runner(String name, boolean slave, boolean partition) {
		super();
		this.name = name;
		this.bytes = name.toLowerCase().getBytes();
		this.slave = slave;
		this.partition = partition;
	}

	public byte[] bytes() {
		return bytes;
	}

	public boolean isSlave() {
		return slave;
	}

	public boolean isPartition() {
		return partition;
	}

}
