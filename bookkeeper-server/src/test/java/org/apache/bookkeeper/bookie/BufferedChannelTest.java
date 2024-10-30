package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.bookkeeper.bookie.utils.AllocatorStatus;
import org.apache.bookkeeper.bookie.utils.FileChannelStaus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;

@RunWith(Parameterized.class)
public class BufferedChannelTest {
    private BufferedChannel bufferedChannelTest;
    private ByteBufAllocator allocatorTest;
    private AllocatorStatus allocatorStatusTest;
    private FileChannel fileChannelTest;
    private FileChannelStaus fileChannelStatusTest;
    private int writeCapacityTest;
    private int readCapacityTest;
    private long unpersistedBytesBoundTest;
    private Class<? extends Exception> exceptionOutputTest;
    private Class<? extends Exception> exceptionTest;
    private final static Path PATH_FC = Paths.get("src/test/java/org/apache/bookkeeper/bookie/utils/filechannel.txt");
    private Set<PosixFilePermission> originalPermissions;

    public BufferedChannelTest(
                               AllocatorStatus allocatorStatusTest,
                               FileChannelStaus fileChannelStatusTest,
                               int writeCapacityTest,
                               int readCapacityTest,
                               long unpersistedBytesBoundTest,
                               Class<? extends Exception> exceptionOutputTest) {
        this.allocatorStatusTest = allocatorStatusTest;
        this.fileChannelStatusTest = fileChannelStatusTest;
        this.writeCapacityTest = writeCapacityTest;
        this.readCapacityTest = readCapacityTest;
        this.unpersistedBytesBoundTest = unpersistedBytesBoundTest;
        this.exceptionOutputTest = exceptionOutputTest;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() throws IOException {
        return Arrays.asList(new Object[][]{
                // Allocator                        FileChannel                         writeCapacity   readCapacity unpersistedBytesBound  Expected Exception
                {AllocatorStatus.NULL,              FileChannelStaus.DEFAULT,           0,              1,           1,                     NullPointerException.class},
                {AllocatorStatus.DEFAULT,           FileChannelStaus.NULL,              1,              0,           -1,                    NullPointerException.class},
                {AllocatorStatus.DEFAULT,           FileChannelStaus.CLOSE,             0,              1,           0,                     java.nio.channels.ClosedChannelException.class},
                {AllocatorStatus.DEFAULT,           FileChannelStaus.DEFAULT,           -1,             0,           1,                     IllegalArgumentException.class},
                {AllocatorStatus.DEFAULT,           FileChannelStaus.DEFAULT,           0,              -1,          0,                     IllegalArgumentException.class},
                {AllocatorStatus.DEFAULT,           FileChannelStaus.DEFAULT,           1,              1,           1,                     null},
                {AllocatorStatus.DEFAULT,           FileChannelStaus.DEFAULT,           1,              1,           -1,                    null},
                {AllocatorStatus.INVALID,           FileChannelStaus.DEFAULT,           0,              0,           -1,                    NullPointerException.class},
                {AllocatorStatus.DEFAULT,           FileChannelStaus.INVALID,           1,              1,           1,                     java.nio.file.AccessDeniedException.class},
                // Add for kill pit mutation
                {AllocatorStatus.DEFAULT,           FileChannelStaus.DEFAULT,           1,              1,           0,                     null},
        });
    }

    @Before
    public void setup() {
        try {
            setupFileChannel(fileChannelStatusTest);
            setupAllocator(allocatorStatusTest);
        } catch (IOException e) {
            exceptionTest = e.getClass();
        }
    }

    @Test
    public void test() {
        if (exceptionTest != null){
            Assert.assertEquals(exceptionOutputTest, exceptionTest);
        }
        else {
            try {
                bufferedChannelTest = spy(new BufferedChannel(allocatorTest, fileChannelTest, writeCapacityTest, readCapacityTest, unpersistedBytesBoundTest));
                Assert.assertNotNull(bufferedChannelTest);
                // Add for kill pit mutation
                Assert.assertEquals(fileChannelTest.position(), bufferedChannelTest.writeBufferStartPosition.get());
                // Use Reflection for access `doRegularFlushes`
                Field doRegularFlushesField = BufferedChannel.class.getDeclaredField("doRegularFlushes");
                doRegularFlushesField.setAccessible(true); // Rendi accessibile il campo privato
                boolean doRegularFlushesValue = (boolean) doRegularFlushesField.get(bufferedChannelTest);
                Assert.assertEquals(unpersistedBytesBoundTest > 0, doRegularFlushesValue);
            } catch (Exception e) {
                Assert.assertEquals(exceptionOutputTest, e.getClass());
            }
        }
    }


    private void setupFileChannel(FileChannelStaus status) throws IOException {
        switch (status) {
            case DEFAULT:
                fileChannelTest = FileChannel.open(PATH_FC);
                fileChannelTest.position(5);
                break;
            case CLOSE:
                fileChannelTest = FileChannel.open(PATH_FC);
                fileChannelTest.close();
                break;
            case INVALID:
                originalPermissions = Files.getPosixFilePermissions(PATH_FC);
                Files.setPosixFilePermissions(PATH_FC, new HashSet<>());
                fileChannelTest = FileChannel.open(PATH_FC);
                break;
            default:
                fileChannelTest = null;
                break;
        }
    }

    private void setupAllocator(AllocatorStatus status) {
        switch (status) {
            case DEFAULT:
                allocatorTest = UnpooledByteBufAllocator.DEFAULT;
                break;
            case INVALID:
                allocatorTest = mock(ByteBufAllocator.class);
                when(allocatorTest.directBuffer(anyInt())).thenReturn(null);
                break;
            default:
                allocatorTest = null;
                break;
        }
    }

    @After
    public void tearDown() throws IOException {
        if (fileChannelTest != null && fileChannelTest.isOpen()) {
            fileChannelTest.close();
        }
        if (bufferedChannelTest != null) {
            bufferedChannelTest.close();
        }
        if (fileChannelStatusTest == FileChannelStaus.INVALID && originalPermissions != null) {
            Files.setPosixFilePermissions(PATH_FC, originalPermissions);
        }
    }

}
