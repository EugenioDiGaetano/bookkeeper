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
                // Src                                  Dst                             UnpersistedBytesBoundExpected       Exception
                {ByteBufStatus.NULL,                    ByteBufStatus.DEFAULT,          16,                                 NullPointerException.class},
                {ByteBufStatus.DEFAULT,                 ByteBufStatus.DEFAULT,          16,                                 null},
                {ByteBufStatus.FULL,                    ByteBufStatus.DEFAULT,          16,                                 java.nio.channels.NonWritableChannelException.class},
                {ByteBufStatus.ZERO_CAPACITY,           ByteBufStatus.DEFAULT,          16,                                 null},
                {ByteBufStatus.DEFAULT,                 ByteBufStatus.ONLY_READ,        -1,                                 null},
                {ByteBufStatus.DEFAULT,                 ByteBufStatus.ZERO_CAPACITY,    16,                                 NonWritableChannelException.class},
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
                fileChannel.truncate(0);  // Prova a svuotare il file solo se è scrivibile
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
                // Memorizza la posizione iniziale del file prima di scrivere
                long initialPosition = fileChannel.position();
                // Esegui la scrittura
                bufferedChannel.write(srcBufferTest);
                // Verifica la posizione finale
                long finalPosition = fileChannel.position();

                if (exceptionOutputTest == null) {
                    // Se non ci sono eccezioni attese, verifica che il file sia stato scritto
                    if (srcStatusTest != ByteBufStatus.ZERO_CAPACITY) {
                        Assert.assertEquals(finalPosition, initialPosition);
                    } else {
                        // Per buffer di capacità zero, non ci aspettiamo modifiche
                        Assert.assertEquals(initialPosition, finalPosition);
                    }
                }

            } catch (Exception e) {
                // Verifica che l'eccezione lanciata sia quella attesa
                Assert.assertEquals(exceptionOutputTest, e.getClass());
            }
        }
    }

    private void setupSrc(ByteBufStatus status) {
        switch (status) {
            case DEFAULT:
                srcBufferTest = Unpooled.buffer(8192).writeBytes(new byte[10]);
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
                //capacity 0 causa un loop infinito nella funzione flush
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
