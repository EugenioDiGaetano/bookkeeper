package org.apache.bookkeeper.bookie.utils;

import io.netty.buffer.ByteBuf;
import org.junit.Assert;

import java.util.Random;

public class Utils {

    public static byte[] generateRandomBytes(int numBytes) {
        byte[] randomBytes = new byte[numBytes];
        Random random = new Random();
        random.nextBytes(randomBytes);
        return randomBytes;
    }

    public static void verifyWrittenBytes(long bytesWritten, long initialPosition, ByteBuf src, byte [] writeBytes) {
        byte[] actualWrittenData = new byte[(int) bytesWritten];
        src.getBytes((int) initialPosition, actualWrittenData);
        byte[] expectedWrittenData = new byte[(int) bytesWritten];
        System.arraycopy(writeBytes, 0, expectedWrittenData, 0, (int) bytesWritten);
        Assert.assertArrayEquals("I dati scritti non corrispondono a quelli attesi.",
                expectedWrittenData,
                actualWrittenData);
    }

}
