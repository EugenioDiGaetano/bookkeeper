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

import static org.apache.bookkeeper.bookie.utils.Utils.generateRandomBytes;
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
    private int fCapacity;
    private byte[] byteFile;
    private byte[] writeBytes;


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
                {ByteBufStatus.INVALID,                 5,                    24,            BufferedInit.BUFFERED_READ_CHANNEL,                        io.netty.util.IllegalReferenceCountException.class},
                //Aggiunti dopo
                {ByteBufStatus.DEFAULT,                 2049,                 1,             BufferedInit.NULL,                                         null},
                {ByteBufStatus.DEFAULT,                 0,                    100,           BufferedInit.NULL,                                         null},
                {ByteBufStatus.DEFAULT,                 2,                    24,            BufferedInit.BUFFERED_CHANNEL,                             null},
                {ByteBufStatus.DEFAULT,                 2,                    68,            BufferedInit.BUFFERED_READ_CHANNEL,                        null}
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
                writeBytes = generateRandomBytes(capacity);
                bufferedChannel = spy(new BufferedChannel(allocator, fileChannel, capacity, 64, unpersistedBytesBoundTest));
                AtomicLong writeBufferStartPositionSpy = spy(bufferedChannel.writeBufferStartPosition);
                Mockito.doReturn(Long.MAX_VALUE).when(writeBufferStartPositionSpy).get();
                bufferedReadChannel.readBuffer.writeBytes(writeBytes);
                bufferedChannel.writeBufferStartPosition = writeBufferStartPositionSpy;
                break;
            case BUFFERED_CHANNEL:
                writeBytes = generateRandomBytes(capacity);
                bufferedChannel = spy(new BufferedChannel(allocator, fileChannel, capacity, 64, unpersistedBytesBoundTest));
                bufferedChannel.writeBuffer.writeBytes(writeBytes);
                break;
            default:
                allocator = spy(UnpooledByteBufAllocator.DEFAULT);
                Mockito.when(allocator.directBuffer(ArgumentMatchers.anyInt())).thenReturn(null);

                bufferedChannel = spy(new BufferedChannel(allocator , fileChannel, capacity, 64, unpersistedBytesBoundTest));
                break;
        }
    }

    private void writeFile(Path pathFc) throws IOException {
        fCapacity = 2048;
        fileChannel = FileChannel.open(pathFc, StandardOpenOption.READ, StandardOpenOption.WRITE);
        byteFile = generateRandomBytes(fCapacity);
        fileChannel.write(ByteBuffer.wrap(byteFile));
        fileChannel.force(true);
        fileChannel.close();
    }



    @Test
    public void test() {
        if (exceptionTest != null) {
            Assert.assertEquals(exceptionOutputTest, exceptionTest);
        } else {
            try {
                // Inizializzazione delle variabili
                int totalBytesWritten = 0;
                int expectedBytesToRead = 0;
                int offset = 0;

                // Calcolo dei byte totali letti
                int bytesRead = bufferedChannel.read(destBufferTest, posTest, lengthTest);

                if (lengthTest > 0) {
                    // Inizializzo expectedBytes con una lunghezza sufficiente
                    byte[] expectedBytes = new byte[Math.max(fCapacity + capacity, lengthTest)];

                    // Calcolo del numero di byte disponibili nel writeBuffer (solo se inizializzato)
                    if (bufferedChannel.writeBuffer != null && bufferedInitStatus != BufferedInit.NULL) {
                        offset = (int) (posTest - bufferedChannel.writeBufferStartPosition.get()); // Usa pos per calcolare offset
                        totalBytesWritten += offset;

                        // Verifica che non superi la dimensione dell'array
                        int bytesToCopy = Math.min(writeBytes.length - offset, expectedBytes.length - totalBytesWritten);
                        if (bytesToCopy > 0) {
                            System.arraycopy(writeBytes, offset, expectedBytes, totalBytesWritten, bytesToCopy);
                            totalBytesWritten += bytesToCopy;
                        }
                    }

                    // Calcolo del numero di byte disponibili nel readBuffer (solo se inizializzato)
                    if (bufferedReadChannel.readBuffer != null && bufferedInitStatus != BufferedInit.NULL) {
                        offset = (int) (posTest - bufferedReadChannel.readBufferStartPosition);
                        totalBytesWritten += offset;

                        // Verifica che non superi la dimensione dell'array
                        int bytesToCopy = Math.min(writeBytes.length - offset, expectedBytes.length - totalBytesWritten);
                        if (bytesToCopy > 0) {
                            System.arraycopy(writeBytes, offset, expectedBytes, totalBytesWritten, bytesToCopy);
                            totalBytesWritten += bytesToCopy;
                        }
                    }

                    // Se rimangono ancora byte da leggere, considera i byte dal file (solo se inizializzato)
                    if (lengthTest > totalBytesWritten && bufferedInitStatus != BufferedInit.NULL) {
                        int bytesFromFile = Math.min(lengthTest - totalBytesWritten, byteFile.length);
                        expectedBytesToRead = totalBytesWritten + bytesFromFile;

                        // Verifica che non superi la dimensione dell'array
                        int bytesToCopy = Math.min(bytesFromFile, expectedBytes.length - totalBytesWritten);
                        if (bytesToCopy > 0) {
                            System.arraycopy(byteFile, 0, expectedBytes, totalBytesWritten, bytesToCopy);
                            totalBytesWritten += bytesToCopy;
                        }
                    } else {
                        expectedBytesToRead = totalBytesWritten;
                    }

                    // Posso leggere al massimo len
                    expectedBytesToRead = Math.min(lengthTest, expectedBytesToRead);

                    // Non posso leggere un valore non positivo
                    expectedBytesToRead = Math.max(expectedBytesToRead, 0);

                    // Verifica e correzione degli indici prima di chiamare Arrays.copyOfRange
                    int startIdx = (int) posTest;
                    int endIdx = (int) (posTest + expectedBytesToRead);
                    if (startIdx < 0) startIdx = 0;
                    if (endIdx > expectedBytes.length) endIdx = expectedBytes.length;

                    // Tronco array
                    if (bufferedInitStatus == BufferedInit.BUFFERED_CHANNEL) {
                        expectedBytes = Arrays.copyOfRange(expectedBytes, startIdx, endIdx);
                    } else {
                        if (byteFile != null && startIdx < byteFile.length) {
                            expectedBytes = Arrays.copyOfRange(byteFile, startIdx, Math.min(endIdx, byteFile.length));
                        } else {
                            expectedBytes = new byte[0]; // Se non ci sono dati nel byteFile, lascio l'array vuoto
                        }
                    }

                    // Verifica del numero di byte letti
                    Assert.assertEquals(expectedBytesToRead, bytesRead);

                    // Copia i dati effettivamente letti
                    byte[] actualBytes = new byte[bytesRead];
                    destBufferTest.getBytes(0, actualBytes, 0, bytesRead);

                    // Verifica che i dati effettivamente letti corrispondano ai dati attesi
                    Assert.assertArrayEquals("I dati letti non corrispondono a quelli attesi.", expectedBytes, actualBytes);
                } else {
                    Assert.assertEquals(expectedBytesToRead, bytesRead);
                }
            } catch (Exception e) {
                if (exceptionOutputTest != null) {
                    Assert.assertTrue("Eccezione attesa non corrisponde: " + e.getClass(), exceptionOutputTest.isInstance(e));
                } else {
                    Assert.fail("Non ci si aspettava un'eccezione, ma è stata lanciata: " + e.getClass() + " " + e.getMessage());
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
            case INVALID:
                destBufferTest = Unpooled.buffer(0);
                destBufferTest.release();
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