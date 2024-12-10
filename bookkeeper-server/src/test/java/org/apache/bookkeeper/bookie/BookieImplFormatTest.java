package org.apache.bookkeeper.bookie;


import org.apache.bookkeeper.bookie.utils.DirStatus;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.IOUtils;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.MockedStatic;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.mockito.Mockito.*;

@RunWith(Parameterized.class)
public class BookieImplFormatTest {

    private final DirStatus journalDirTest;
    private final DirStatus ledgerDirTest;
    private final DirStatus indexDirTest;
    private final DirStatus metadataDirTest;
    private final boolean isInteractive;
    private final boolean force;
    private final boolean validPrompt;
    private final boolean expectedOutput;

    private ServerConfiguration configuration;
    private final List<File> tempDirs = new ArrayList<>();
    MockedStatic<IOUtils> mockedStatic;

    public BookieImplFormatTest(DirStatus journalDirTest, DirStatus ledgerDirTest, DirStatus indexDirTest,
                                DirStatus metadataDirTest, boolean isInteractive, boolean force, boolean validPrompt,boolean expectedOutput) {
        this.journalDirTest = journalDirTest;
        this.ledgerDirTest = ledgerDirTest;
        this.indexDirTest = indexDirTest;
        this.metadataDirTest = metadataDirTest;
        this.isInteractive = isInteractive;
        this.force = force;
        this.validPrompt = validPrompt;
        this.expectedOutput = expectedOutput;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                // journalDir               ledgerDir                   indexDir                 metadataDir            isInteractive           force       userValidPrompt         Output
                {DirStatus.INVALID,         DirStatus.VALID,            DirStatus.VALID,         DirStatus.VALID,       true,                   false,      true,                   false},
                {DirStatus.VALID,           DirStatus.INVALID,          DirStatus.VALID,         DirStatus.VALID,       false,                  false,      true,                   false},
                {DirStatus.VALID,           DirStatus.VALID,            DirStatus.INVALID,       DirStatus.VALID,       false,                  true,       true,                   false},
                {DirStatus.VALID,           DirStatus.VALID,            DirStatus.VALID,         DirStatus.INVALID,     false,                  true,       true,                   false},
                {DirStatus.VALID,           DirStatus.VALID,            DirStatus.VALID,         DirStatus.VALID,       false,                  true,       true,                   true},
                {DirStatus.VALID,           DirStatus.VALID,            DirStatus.VALID,         DirStatus.VALID,       false,                  false,      true,                   false},
                {DirStatus.VALID,           DirStatus.VALID,            DirStatus.VALID,         DirStatus.VALID,       true,                   true,       true,                   true},
                {DirStatus.VALID,           DirStatus.VALID,            DirStatus.VALID,         DirStatus.VALID,       true,                   false,      true,                   true},
                {DirStatus.MISSING,         DirStatus.MISSING,          DirStatus.MISSING,       DirStatus.MISSING,     true,                   true,       true,                   true},
                {DirStatus.EMPTY,           DirStatus.EMPTY,            DirStatus.EMPTY,         DirStatus.EMPTY,       true,                   true,       true,                   true},
                {DirStatus.NULL,            DirStatus.NULL,             DirStatus.NULL,          DirStatus.NULL,        true,                   true,       true,                   true},
                //Aggiunti dopo,
                {DirStatus.FILE,            DirStatus.VALID,            DirStatus.VALID,         DirStatus.VALID,       true,                   false,      true,                   true},
                {DirStatus.VALID,           DirStatus.FILE,             DirStatus.VALID,         DirStatus.VALID,       false,                  false,      true,                   false},
                {DirStatus.VALID,           DirStatus.VALID,            DirStatus.FILE,          DirStatus.VALID,       false,                  true,       true,                   true},
                {DirStatus.VALID,           DirStatus.INVALID,          DirStatus.VALID,         DirStatus.VALID,       false,                  true,       true,                   false},
                {DirStatus.VALID,           DirStatus.VALID,            DirStatus.VALID,         DirStatus.VALID,       true,                   false,      false,                  false},
                //Aggiunto per eliminare mutazione
                {DirStatus.VALID,           DirStatus.EMPTY,            DirStatus.EMPTY,         DirStatus.EMPTY,       false,                  false,      true,                   false}
        });
    }

    @Before
    public void setup() throws IOException {
        configuration = new ServerConfiguration();
        setupDirectories(journalDirTest, dirs -> {configuration.setJournalDirsName(dirs);});
        setupDirectories(ledgerDirTest, dirs -> {configuration.setLedgerDirNames(dirs);});
        setupDirectories(indexDirTest, dirs -> {configuration.setIndexDirName(dirs);});
        configuration.setGcEntryLogMetadataCachePath(setupDirectory(metadataDirTest));
        if (validPrompt) {
            mockedStatic = mockStatic(IOUtils.class);
            mockedStatic.when(() -> IOUtils.confirmPrompt(anyString())).thenReturn(true);
        } else {
            InputStream mockInputStream = mock(InputStream.class);
            when(mockInputStream.read()).thenThrow(new IOException("Simulated IOException"));
            System.setIn(mockInputStream);
        }
    }

    @Test
    public void test() {
        boolean output = BookieImpl.format(configuration, isInteractive, force);
        Assert.assertEquals("Unexpected output for test case", expectedOutput, output);
        Assert.assertTrue("Directory validation failed", validateDirectories(expectedOutput, tempDirs));
    }

    private boolean validateDirectories(boolean methodOutput, List<File> directories) {
        if (methodOutput) {
            for (File dir : directories) {
                if (dir.exists() && !dir.isDirectory() && dir.list().length > 0) {
                    return false;
                }
            }
            return true;
        } else {
            for (File dir : directories) {
                if (dir.exists() && dir.isDirectory() && dir.list().length > 0) {
                    return true;
                }
            }
            return false;
        }
    }

    @After
    public void cleanup() {
        for (File dir : tempDirs) {
            if (dir != null && dir.exists()) {
                dir.setWritable(true);
                deleteRecursively(dir);
            }
        }
        if (mockedStatic != null) {
            mockedStatic.close();
        }
        System.setIn(System.in);
    }

    private String setupDirectory(DirStatus status) {
        String dirPath = null;
        switch (status) {
            case VALID:
                File validDir = new File(System.getProperty("java.io.tmpdir"), "valid_" + System.nanoTime());
                validDir.mkdirs();
                tempDirs.add(validDir);
                try {
                    new File(validDir, "testfile1.log").createNewFile();
                    new File(validDir, "testfile2.index").createNewFile();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                dirPath = validDir.getAbsolutePath();
                break;
            case EMPTY:
                File emptyDir = new File(System.getProperty("java.io.tmpdir"), "empty_" + System.nanoTime());
                emptyDir.mkdirs();
                tempDirs.add(emptyDir);
                dirPath = emptyDir.getAbsolutePath();
                break;
            case MISSING:
                dirPath = new File(System.getProperty("java.io.tmpdir"), "missing_" + System.nanoTime()).getAbsolutePath();
                break;
            case INVALID:
                File invalidDir = new File(System.getProperty("java.io.tmpdir"), "invalid_" + System.nanoTime());
                invalidDir.mkdirs();
                tempDirs.add(invalidDir);
                try {
                    new File(invalidDir, "testfile1.log").createNewFile();
                    new File(invalidDir, "testfile2.index").createNewFile();
                    invalidDir.setWritable(false);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                dirPath = invalidDir.getAbsolutePath();
                break;
            case FILE:
                File fileDir = new File(System.getProperty("java.io.tmpdir"), "file_" + System.nanoTime());
                fileDir.mkdirs();
                tempDirs.add(fileDir);
                try {
                    File file = new File(fileDir, "fileDir.log");
                    file.createNewFile();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                dirPath = fileDir.getAbsolutePath()+"/fileDir.log";
                break;
            default:
                dirPath = null;
                break;
        }
        return dirPath;
    }

    private void setupDirectories(DirStatus status, DirectorySetter setter) {
        String[] dirPaths = new String[4];
        switch (status) {
            case VALID:
                dirPaths[0] = setupDirectory(DirStatus.VALID);
                dirPaths[1] = setupDirectory(DirStatus.MISSING);
                dirPaths[2] = setupDirectory(DirStatus.EMPTY);
                break;
            case INVALID:
                dirPaths[0] = setupDirectory(DirStatus.VALID);
                dirPaths[1] = setupDirectory(DirStatus.MISSING);
                dirPaths[2] = setupDirectory(DirStatus.EMPTY);
                dirPaths[3] = setupDirectory(DirStatus.INVALID);
                break;
            case FILE:
                dirPaths[0] = setupDirectory(DirStatus.VALID);
                dirPaths[1] = setupDirectory(DirStatus.MISSING);
                dirPaths[2] = setupDirectory(DirStatus.EMPTY);
                dirPaths[3] = setupDirectory(DirStatus.FILE);
                break;
            default:
                dirPaths = new String[0];
                break;
        }
        setter.setDirectories(dirPaths);
    }

    private void deleteRecursively(File file) {
        if (file == null || !file.exists()) {
            return;
        }
        if (file.isDirectory()) {
            for (File child : file.listFiles()) {
                deleteRecursively(child);
            }
        }
        file.delete();
    }

    @FunctionalInterface
    private interface DirectorySetter {
        void setDirectories(String[] dirs);
    }
}
