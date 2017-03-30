package org.mariadb.jdbc.internal.util.buffer;

import org.junit.Assert;
import org.junit.Test;
import org.mariadb.jdbc.internal.com.read.Buffer;

public class BufferTest {

    @Test
    public void testGetLengthEncodedBinary() throws Exception {
        Assert.assertEquals(15, new Buffer(new byte[]{(byte)0x0f}).getLengthEncodedNumeric());
        Assert.assertEquals(65535, new Buffer(new byte[]{(byte)0xfc, (byte)0xff, (byte)0xff}).getLengthEncodedNumeric());
        Assert.assertEquals(16777215, new Buffer(new byte[]{(byte)0xfd, (byte)0xff, (byte)0xff, (byte)0xff}).getLengthEncodedNumeric());
        Assert.assertEquals(Long.MAX_VALUE, new Buffer(new byte[]{(byte)0xfe, (byte)0xff, (byte)0xff, (byte)0xff, (byte)0xff, (byte)0xff,
                (byte)0xff, (byte)0xff, (byte)0x7f}).getLengthEncodedNumeric());
    }

    @Test
    public void testSkipLengthEncodedBytes() throws Exception {
        byte[] arr15 = new byte[1];
        arr15[0] = (byte)0x0f;
        Buffer buf15 = new Buffer(arr15);
        buf15.skipLengthEncodedBytes();
        Assert.assertEquals(16, buf15.position);

        byte[] arr2 = new byte[3];
        arr2[0] = (byte)0xfc;
        arr2[1] = (byte)0xff;
        arr2[2] = (byte)0xff;
        Buffer buf2 = new Buffer(arr2);
        buf2.skipLengthEncodedBytes();
        Assert.assertEquals(65538, buf2.position);

        byte[] arr3 = new byte[4];
        arr3[0] = (byte)0xfd;
        arr3[1] = (byte)0xff;
        arr3[2] = (byte)0xff;
        arr3[3] = (byte)0xff;
        Buffer buf3 = new Buffer(arr3);
        buf3.skipLengthEncodedBytes();
        Assert.assertEquals(16777215 + 4, buf3.position);

        byte[] arr4 = new byte[9];
        arr4[0] = (byte)0xfe;
        arr4[1] = (byte)0xf0;
        arr4[2] = (byte)0xff;
        arr4[3] = (byte)0xff;
        arr4[4] = (byte)0x7f;
        arr4[5] = (byte)0x00;
        arr4[6] = (byte)0x00;
        arr4[7] = (byte)0x00;
        arr4[8] = (byte)0x00;
        Buffer buf4 = new Buffer(arr4);
        buf4.skipLengthEncodedBytes();
        Assert.assertEquals(Integer.MAX_VALUE - 15 + 9, buf4.position);

    }

}