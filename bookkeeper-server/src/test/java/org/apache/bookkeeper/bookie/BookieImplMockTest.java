package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.junit.Assert;
import org.junit.Test;

import java.net.UnknownHostException;

import static org.mockito.Mockito.*;
import org.mockito.MockedConstruction;

public class BookieImplMockTest {
    private BookieSocketAddress bookieImplTest;
    private final int portTest = 0;
    private final String hostName = "192.168.1.100";


    @Test
    public void testMockConstructionNoLook() {
        BookieSocketAddress myAdd = new BookieSocketAddress(hostName, portTest);

        try (MockedConstruction<BookieSocketAddress> mocked = mockConstruction(BookieSocketAddress.class, (mock, context) -> {
            when(mock.getSocketAddress()).thenReturn(myAdd.getSocketAddress());
            when(mock.getPort()).thenReturn(myAdd.getPort());
        })) {
            ServerConfiguration confTest = new ServerConfiguration();
            confTest.setAdvertisedAddress("");
            confTest.setListeningInterface("default");
            confTest.setBookiePort(1);
            confTest.setAllowLoopback(false);
            confTest.setUseHostNameAsBookieID(false);
            confTest.setUseShortHostName(false);
            try {
                bookieImplTest = BookieImpl.getBookieAddress(confTest);
                Assert.assertEquals("Porta attesa: " + portTest, portTest, bookieImplTest.getSocketAddress().getPort());
                Assert.assertEquals("Nome host atteso: " + hostName , hostName, bookieImplTest.getSocketAddress().getHostName());

            } catch (UnknownHostException e) {
                Assert.fail("Eccezione inattesa:" + e.getClass());
            }
        }
    }
}
