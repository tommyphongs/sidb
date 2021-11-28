package org.ptm.sidb.utils;

import org.ptm.sidb.SiDBUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.*;

public class DataModeling {
    public static class DefaultDataBlock implements DataBlock {

        public static DataBlock me = new DefaultDataBlock();
        public static byte[] NULL = "null".getBytes();
        public static byte[][] NULLs = new byte[][]{};

        protected Charset charset = SiDBUtils.DEFAULT_CHARSET;

        public byte[] toBytes(Object obj) {
            if (obj == null)
                return NULL;
            if (obj instanceof byte[])
                return (byte[]) obj;
            if (obj instanceof ArrayBytes)
                return ((ArrayBytes)obj).toBytes();
            if (obj instanceof InputStream) {
                InputStream in = (InputStream)obj;
                byte[] data;
                try {
                    data = new byte[in.available()];
                    in.read(data);
                } catch (IOException e) {
                    throw new SiDBException(e);
                } finally {
                    try {
                        in.close();
                    } catch (IOException e) {
                    }
                }
                return data;
            }
            return obj.toString().getBytes(charset);
        }

        @SuppressWarnings({ "unchecked", "rawtypes" })
        public byte[][] toBytes(Object... objs) {
            if (objs == null)
                return NULLs;
            if (objs instanceof byte[][])
                return (byte[][]) objs;
            if (objs.length == 1) {
                Object arg = objs[0];
                if (arg instanceof Map) {
                    Map<Object, Object> map = (Map<Object, Object>) arg;
                    byte[][] args = new byte[map.size() * 2][];
                    int i = 0;
                    for (Map.Entry<Object, Object> en : map.entrySet()) {
                        args[i] = toBytes(en.getKey());
                        args[i + 1] = toBytes(en.getValue());
                        i += 2;
                    }
                    return args;
                }
                if (arg instanceof Collection) {
                    arg = ((Collection)arg).iterator();
                }
                if (arg instanceof Iterator) {
                    List<byte[]> list = new ArrayList<byte[]>();
                    Iterator it = (Iterator)arg;
                    while (it.hasNext()) {
                        list.add(toBytes(it.next()));
                    }
                    return list.toArray(new byte[list.size()][]);
                }
                return new byte[][]{toBytes(arg)};
            }
            byte[][] args = new byte[objs.length][];
            for (int i = 0; i < args.length; i++) {
                args[i] = toBytes(objs[i]);
            }
            return args;
        }
    }

    public interface DataBlock {

        byte[] toBytes(Object obj);

        byte[][] toBytes(Object... objs);
    }
}
