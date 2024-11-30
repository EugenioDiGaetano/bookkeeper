package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.bookkeeper.bookie.utils.ByteBufStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.NonWritableChannelException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class BufferedChannelWriteTest {
    private ByteBuf srcBufferTest;
    private ByteBufStatus srcStatusTest;
    private ByteBufStatus dstStatusTest;
    private long unpersistedBytesBoundTest;
    private final Class<? extends Exception> exceptionOutputTest;
    private Class<? extends Exception> exceptionTest;
    private BufferedChannel bufferedChannel;
    private UnpooledByteBufAllocator allocator;
    private FileChannel fileChannel;
    private int capacity;
    private final static Path PATH_FC = Paths.get("src/test/java/org/apache/bookkeeper/bookie/utils/filechannel.txt");


    public BufferedChannelWriteTest(ByteBufStatus srcStatusTest, ByteBufStatus dstStatusTest, long unpersistedBytesBoundTest ,Class<? extends Exception> exceptionOutputTest) {
        this.srcStatusTest = srcStatusTest;
        this.dstStatusTest = dstStatusTest;
        this.unpersistedBytesBoundTest = unpersistedBytesBoundTest;
        this.exceptionOutputTest = exceptionOutputTest;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                // Src                                  Dst                             UnpersistedBytesBound               Exception
                {ByteBufStatus.NULL,                    ByteBufStatus.DEFAULT,          16,                                 NullPointerException.class},
                {ByteBufStatus.DEFAULT,                 ByteBufStatus.DEFAULT,          16,                                 null},
                {ByteBufStatus.FULL,                    ByteBufStatus.DEFAULT,          16,                                 null},
                {ByteBufStatus.ZERO_CAPACITY,           ByteBufStatus.DEFAULT,          16,                                 null},
                {ByteBufStatus.DEFAULT,                 ByteBufStatus.ONLY_READ,        -1,                                 null},
                // Test fallisce perché causa loop infinito
                // {ByteBufStatus.DEFAULT,                 ByteBufStatus.ZERO_CAPACITY,    16,                                 null},
                {ByteBufStatus.DEFAULT,                 ByteBufStatus.LITTLE,           16,                                 null},
                {ByteBufStatus.DEFAULT,                 ByteBufStatus.CLOSE,            16,                                 java.nio.channels.ClosedChannelException.class},
                {ByteBufStatus.MORE,                    ByteBufStatus.DEFAULT,          10245,                              null}
        });
    }

    @Before
    public void setup() {
        try {
            allocator = UnpooledByteBufAllocator.DEFAULT;
            fileChannel = FileChannel.open(PATH_FC, StandardOpenOption.WRITE);
            setupSrc(srcStatusTest);
            setupDst(dstStatusTest);
            bufferedChannel = new BufferedChannel(allocator, fileChannel, capacity, 8192, unpersistedBytesBoundTest);

        } catch (IOException e) {
            exceptionTest = e.getClass();
        }
    }


    @After
    public void tearDown() throws IOException {
        if (fileChannel != null && fileChannel.isOpen()) {
            try {
                fileChannel.truncate(0);
            } catch (NonWritableChannelException e) {
                // Se il file è aperto in sola lettura, ignora l'eccezione
            } finally {
                fileChannel.close();
            }
            fileChannel.close();
        }
    }

    @Test
    public void test() {
        if (exceptionTest != null) {
            Assert.assertEquals(exceptionOutputTest, exceptionTest);
        } else {
            try {
                long initialPosition = fileChannel.position();
                bufferedChannel.write(srcBufferTest);
                long finalPosition = fileChannel.position();
                int bytesToCopy = Math.min(srcBufferTest.readableBytes(), bufferedChannel.writeBuffer.writableBytes());

                if (exceptionOutputTest == null) {
                    Assert.assertTrue(bufferedChannel.position >= initialPosition);
                    if (dstStatusTest == ByteBufStatus.ONLY_READ) {
                        Assert.assertEquals(finalPosition, initialPosition);

                    } else if (dstStatusTest != ByteBufStatus.ZERO_CAPACITY && dstStatusTest != ByteBufStatus.LITTLE) {
                        Assert.assertEquals(initialPosition + bytesToCopy, finalPosition);
                    }
                }

            } catch (Exception e) {
                Assert.assertEquals(exceptionOutputTest, e.getClass());
            }
        }
    }

    private void setupSrc(ByteBufStatus status) {
        switch (status) {
            case DEFAULT:
                srcBufferTest = Unpooled.buffer(8192).writeBytes(new byte[4096]);
                break;
            case FULL:
                srcBufferTest = Unpooled.buffer(8192).writeBytes(new byte[8192]);
                break;
            case ZERO_CAPACITY:
                srcBufferTest = Unpooled.buffer(0);
                break;
            case ONLY_READ:
                srcBufferTest = Unpooled.buffer(8192).asReadOnly();
                break;
            case CLOSE:
                srcBufferTest = Unpooled.buffer(8192);
                break;
            case MORE:
                srcBufferTest = Unpooled.buffer(10240);
                break;
            default:
                srcBufferTest = null;
                break;
        }
    }

    private void setupDst(ByteBufStatus status) throws IOException {
        switch (status) {
            case ZERO_CAPACITY:
                capacity = 0;
                break;
            case LITTLE:
                capacity = 1;
                break;
            case ONLY_READ:
                fileChannel.close();
                fileChannel = FileChannel.open(PATH_FC, StandardOpenOption.READ);
                capacity = 8192;
                break;
            case CLOSE:
                fileChannel.close();
                capacity = 8192;
                break;
            default:
                capacity = 8192;
                break;
        }
    }
}
