package org.ptm.sidb.utils;

import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.commons.pool.impl.GenericObjectPool.Config;
import org.ptm.sidb.impl.Streams;

public class ConnectionPools {
    
    public static Streams.PoolSiDBStream pool(final String host, final int port, final int timeout, Object cnf) {
        return pool(host, port, timeout, cnf, null);
    }

    public static Streams.PoolSiDBStream pool(final String host, final int port, final int timeout, Object cnf, final byte[] auth) {
        if (cnf == null) {
            Config config = new Config();
            config.maxActive = 10;
            config.testWhileIdle = true;
            cnf = config;
        }
        return new Streams.PoolSiDBStream(new GenericObjectPool<Streams.SiDBStream>(new BasePoolableObjectFactory<Streams.SiDBStream>() {
            
            public Streams.SiDBStream makeObject() throws Exception {
                return new Streams.SocketSiDBStream(host, port, timeout, auth);
            }

            public boolean validateObject(Streams.SiDBStream stream) {
                try {
                    return stream.request(Runner.ping).ok();
                } catch (Exception e) {
                    e.printStackTrace();
                    return false;
                }
            }
            
            public void destroyObject(Streams.SiDBStream obj) throws Exception {
                obj.close();
            }
        }, (Config)cnf));
    }
}
