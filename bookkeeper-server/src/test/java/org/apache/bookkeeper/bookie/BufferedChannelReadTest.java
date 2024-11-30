package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.bookkeeper.bookie.utils.BufferedInit;
import org.apache.bookkeeper.bookie.utils.ByteBufStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.NonWritableChannelException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.Mockito.spy;


@RunWith(Parameterized.class)
public class BufferedChannelReadTest {
    private ByteBuf destBufferTest;
    private int posTest;
    private int lengthTest;
    private ByteBufStatus destStatusTest;
    private  BufferedInit bufferedInitStatus;
    private final Class<? extends Exception> exceptionOutputTest;
    private final static Path PATH_FC = Paths.get("src/test/java/org/apache/bookkeeper/bookie/utils/filechannel.txt");
    private BufferedChannel bufferedChannel;
    private BufferedReadChannel bufferedReadChannel;
    private long unpersistedBytesBoundTest;
    private UnpooledByteBufAllocator allocator;
    private FileChannel fileChannel;
    private int capacity;
    private int byteFile;


    private Class<? extends Exception> exceptionTest;

    public BufferedChannelReadTest(ByteBufStatus destStatusTest, int posTest, int lengthTest, BufferedInit bufferedInitStatus,Class<? extends Exception> exceptionOutputTest) {
        this.destStatusTest = destStatusTest;
        this.posTest = posTest;
        this.lengthTest = lengthTest;
        this.bufferedInitStatus = bufferedInitStatus;
        this.exceptionOutputTest = exceptionOutputTest;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                //Dst                                   Pos                   Length         ReadStatus                                                 Exception
                {ByteBufStatus.NULL,                    0,                    1,             BufferedInit.BUFFERED_CHANNEL,                             NullPointerException.class},
                {ByteBufStatus.NULL,                    0,                    0,             BufferedInit.BUFFERED_READ_CHANNEL,                        null},
                {ByteBufStatus.ZERO_CAPACITY,           0,                    1,             BufferedInit.BUFFERED_CHANNEL,                             java.io.IOException.class},
                {ByteBufStatus.DEFAULT,                 5,                    24,            BufferedInit.BUFFERED_READ_CHANNEL,                        null},
                {ByteBufStatus.DEFAULT,                 -1,                   1,             BufferedInit.BUFFERED_CHANNEL,                             java.lang.IllegalArgumentException.class},
                {ByteBufStatus.DEFAULT,                 0,                    -1,            BufferedInit.BUFFERED_READ_CHANNEL,                        null},
                {ByteBufStatus.ZERO_CAPACITY,           -1,                   1,             BufferedInit.BUFFERED_CHANNEL,                             java.lang.IllegalArgumentException.class},
                {ByteBufStatus.DEFAULT,                 2047,                 1,             BufferedInit.BUFFERED_READ_CHANNEL,                        null},
                {ByteBufStatus.DEFAULT,                 2048,                 1,             BufferedInit.BUFFERED_READ_CHANNEL,                        java.io.IOException.class},
                {ByteBufStatus.DEFAULT,                 2049,                 1,             BufferedInit.BUFFERED_CHANNEL,                             java.lang.IllegalArgumentException.class},
                {ByteBufStatus.DEFAULT,                 0,                    1,             BufferedInit.BUFFERED_CHANNEL,                             null},
                //Added after
                {ByteBufStatus.DEFAULT,                 2049,                 1,             BufferedInit.NULL,                                         null},
                {ByteBufStatus.DEFAULT,                 2,                    24,            BufferedInit.BUFFERED_CHANNEL,                             null},
                {ByteBufStatus.DEFAULT,                 2,                    68,            BufferedInit.BUFFERED_READ_CHANNEL,                        null},
        });
    }

    @Before
    public void setup() {
        try {
            writeFile(PATH_FC);
            setupDest(destStatusTest);
            setupBufferedChannels(bufferedInitStatus);
        } catch (IOException e) {
            exceptionTest = e.getClass();
        }
    }

    private void setupBufferedChannels(BufferedInit status) throws IOException {
        fileChannel = FileChannel.open(PATH_FC, StandardOpenOption.WRITE, StandardOpenOption.READ);
        capacity = 64;
        unpersistedBytesBoundTest = 0;
        allocator = UnpooledByteBufAllocator.DEFAULT;
        bufferedReadChannel = spy(new BufferedReadChannel(fileChannel, capacity));
        switch (status) {
            case BUFFERED_READ_CHANNEL:
                bufferedChannel = spy(new BufferedChannel(allocator, fileChannel, capacity, 64, unpersistedBytesBoundTest));
                AtomicLong writeBufferStartPositionSpy = spy(bufferedChannel.writeBufferStartPosition);
                Mockito.doReturn(Long.MAX_VALUE).when(writeBufferStartPositionSpy).get();
                bufferedReadChannel.readBuffer.writeBytes(new byte[64]);
                bufferedChannel.writeBufferStartPosition = writeBufferStartPositionSpy;
                break;
            case BUFFERED_CHANNEL:
                bufferedChannel = spy(new BufferedChannel(allocator, fileChannel, capacity, 64, unpersistedBytesBoundTest));
                bufferedChannel.writeBuffer.writeBytes(new byte[64]);
                break;
            default:
                allocator = spy(UnpooledByteBufAllocator.DEFAULT);
                Mockito.when(allocator.directBuffer(ArgumentMatchers.anyInt())).thenReturn(null);

                bufferedChannel = spy(new BufferedChannel(allocator , fileChannel, capacity, 64, unpersistedBytesBoundTest));
                break;
        }
    }

    private void writeFile(Path pathFc) throws IOException {
        fileChannel = FileChannel.open(pathFc, StandardOpenOption.READ, StandardOpenOption.WRITE);
        byteFile = 2048;
        fileChannel.write(ByteBuffer.wrap(new byte[byteFile]));
        fileChannel.force(true);
        fileChannel.close();
    }


    @Test
    public void test() {

        if (exceptionTest != null){
            Assert.assertEquals(exceptionOutputTest, exceptionTest);
        }
        else {
            try {
                // Calcolo del totale dei byte scritti nei buffer
                int totalBytesWritten = 0;

                if (bufferedChannel.writeBuffer != null) {
                    totalBytesWritten += bufferedChannel.writeBuffer.writerIndex();
                }

                if (bufferedReadChannel.readBuffer != null) {
                    totalBytesWritten += bufferedReadChannel.readBuffer.writerIndex();
                }

                if (lengthTest - totalBytesWritten > 0 && bufferedReadChannel.readBuffer != null && bufferedChannel.writeBuffer != null) {
                    totalBytesWritten += Math.min(lengthTest - totalBytesWritten, byteFile);
                }
                int bytesRead = bufferedChannel.read(destBufferTest, posTest, lengthTest);

                // Calcolo dei byte attesi da leggere, non posso leggere byte negativi
                if (lengthTest < 0)
                    lengthTest = 0;

                int expectedBytesToRead = Math.min(lengthTest, totalBytesWritten);

                // Verifica che il numero di byte letti sia corretto
                Assert.assertEquals(expectedBytesToRead, bytesRead);



            } catch (Exception e) {

                if (exceptionOutputTest != null) {
                    Assert.assertTrue("Eccezione attesa non corrisponde: " + e.getClass(), exceptionOutputTest.isInstance(e));
                } else {
                    Assert.fail("Non ci si aspettava un'eccezione, ma è stata lanciata: " + e.getClass() + e.getMessage() + e.getLocalizedMessage());
                }
            }
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

    private void setupDest(ByteBufStatus status) throws IOException {
        switch (status) {
            case ZERO_CAPACITY:
                destBufferTest = Unpooled.buffer(0);
                break;
            case DEFAULT:
                if (lengthTest<0)
                    destBufferTest = Unpooled.buffer(0);
                else
                    destBufferTest = Unpooled.buffer(lengthTest);
                break;
            default:
                destBufferTest = null;
                break;
        }

    }

}