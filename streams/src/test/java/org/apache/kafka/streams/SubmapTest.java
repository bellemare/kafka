//package org.apache.kafka.streams;
//
//import java.util.NavigableMap;
//import java.util.TreeMap;
//
//import org.apache.kafka.common.utils.Bytes;
//import org.junit.Test;
//
//import static org.junit.Assert.assertEquals;
//
//public class SubmapTest {
//
//    @Test
//    public void mapTest() {
//        final NavigableMap<Bytes, byte[]> map = new TreeMap<>();
//        Bytes key1 = Bytes.wrap(new byte[]{(byte)0xFE});
//        byte[] val = new byte[]{(byte)0x00};
//        map.put(key1, val);
//
//        Bytes key2 = Bytes.wrap(new byte[]{(byte)0xFE, (byte)0x01});
//        map.put(key2, val);
//
//        Bytes key3 = Bytes.wrap(new byte[]{(byte)0xFE, (byte)0xFF, (byte)0xFF, (byte)0xFF, (byte)0xFF});
//        map.put(key3, val);
//
//        Bytes key4 = Bytes.wrap(new byte[]{(byte)0xFF, (byte)0xFF, (byte)0xFF, (byte)0xFF, (byte)0xFF});
//        map.put(key4, val);
//
//        Bytes key5 = Bytes.wrap(new byte[]{(byte)0xFF});
//        map.put(key5, val);
//
//        Bytes fromKey = Bytes.wrap(new byte[]{(byte)0xFE});
//        Bytes toKey = Bytes.increment(fromKey);
//
//        Bytes result = Bytes.increment(Bytes.wrap(new byte[]{(byte)0x00}));
//        Bytes result2 = Bytes.increment(Bytes.wrap(new byte[]{(byte)0xFF}));
//        Bytes result3 = Bytes.increment(Bytes.wrap(new byte[]{(byte)0xFF, (byte)0xFF, (byte)0xFF, (byte)0xFF}));
//        Bytes result4 = Bytes.increment(Bytes.wrap(new byte[]{(byte)0xFF, (byte)0x00, (byte)0xFF, (byte)0xFF}));
//        NavigableMap<Bytes, byte[]> subMapResults = map.subMap(fromKey, true, toKey, false);
//
//        int ffff = fromKey.compareTo(toKey);
//        int ffff2 = toKey.compareTo(toKey);
//        int ffff3 = toKey.compareTo(fromKey);
//
//
//        NavigableMap<Bytes, byte[]> subMapExpected = new TreeMap<>();
//        subMapExpected.put(key1, val);
//        subMapExpected.put(key2, val);
//        subMapExpected.put(key3, val);
//
//        assertEquals(subMapExpected.keySet(), subMapResults.keySet());
//    }
//}
