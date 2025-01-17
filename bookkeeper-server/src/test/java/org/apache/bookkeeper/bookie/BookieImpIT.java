package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.NullMetadataBookieDriver;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.DiskChecker;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

import static org.junit.Assert.assertEquals;

public class BookieImpIT {

    private BookieImpl bookie;
    private ServerConfiguration serverConfig;

    private static Path ledgerDirectory;
    private static Path indexDirectory;

    @Before
    public void setUp() throws Exception {
        serverConfig = new ServerConfiguration();
        serverConfig.setBookiePort(3181);

        // Creazione delle directory temporanee per ledger e index
        ledgerDirectory = Files.createTempDirectory("ledger-directory");
        indexDirectory = Files.createTempDirectory("index-directory");

        serverConfig.setLedgerDirNames(new String[]{ledgerDirectory.toString()});
        serverConfig.setIndexDirName(new String[]{indexDirectory.toString()});
        serverConfig.setAllowLoopback(true);
        serverConfig.setListeningInterface("lo");

        // Inizializzazione del gestore dei ledger e dello storage
        DiskChecker diskChecker = new DiskChecker(0.95f, 0.94f);
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(serverConfig, serverConfig.getLedgerDirs(), diskChecker);
        LedgerDirsManager indexDirsManager = new LedgerDirsManager(serverConfig, serverConfig.getLedgerDirs(), diskChecker);

        LedgerStorage ledgerStorage = new InterleavedLedgerStorage();
        ledgerStorage.initialize(serverConfig, null, ledgerDirsManager, null, NullStatsLogger.INSTANCE, UnpooledByteBufAllocator.DEFAULT);

        ByteBufAllocator byteBufAllocator = UnpooledByteBufAllocator.DEFAULT;

        bookie = new BookieImpl(
                serverConfig,
                new NullMetadataBookieDriver.NullRegistrationManager(),
                ledgerStorage,
                diskChecker,
                ledgerDirsManager,
                indexDirsManager,
                new NullStatsLogger(),
                byteBufAllocator,
                () -> null
        );
    }

    @Test
    public void testAddAndReadEntry() throws Exception {
        long ledgerId = 100;
        long entryId = 10;
        long lastConfirmed = 12345L;

        bookie.getLedgerStorage().setMasterKey(ledgerId, "master-key".getBytes());

        ByteBuf entryBuffer = Unpooled.buffer(Long.BYTES * 3);
        entryBuffer.writeLong(ledgerId);
        entryBuffer.writeLong(entryId);
        entryBuffer.writeLong(lastConfirmed);

        BookkeeperInternalCallbacks.WriteCallback writeCallback = new BookieImpl.NopWriteCallback();
        bookie.addEntry(entryBuffer, false, writeCallback, "test-context", "master-key".getBytes());

        ByteBuf retrievedBuffer = bookie.readEntry(ledgerId, entryId);

        assertEquals(ledgerId, retrievedBuffer.getLong(0));
        assertEquals(entryId, retrievedBuffer.getLong(Long.BYTES));
        assertEquals(lastConfirmed, retrievedBuffer.getLong(2 * Long.BYTES));
    }

    @AfterClass
    public static void tearDown() {
        cleanUpDirectory(ledgerDirectory);
        cleanUpDirectory(indexDirectory);
    }

    private static void cleanUpDirectory(Path directory) {
        if (directory != null && Files.exists(directory)) {
            try {
                Files.walk(directory)
                        .sorted(Comparator.reverseOrder())
                        .forEach(path -> {
                            try {
                                Files.delete(path);
                            } catch (IOException e) {
                                System.err.println("Errore durante la pulizia della directory: " + e.getMessage());
                            }
                        });
            } catch (IOException e) {
                System.err.println("Errore durante l'accesso alla directory: " + directory);
            }
        }
    }
}
