package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.mockito.Mockito.*;


@RunWith(Parameterized.class)
public class BookieImplgetAddressTest {
    private BookieSocketAddress bookieImplTest;
    private ServerConfiguration confTest;
    private String advertisedAddressTest;
    private int portTest;
    private String listeningInterfaceTest;
    private boolean useHostNameAsBookieIdTest;
    private boolean useShortHostNameTest;
    private boolean allowLoopbackTest;
    private Class<? extends Exception> exceptionOutputTest;
    private Class<? extends Exception> exceptionTest;

    public BookieImplgetAddressTest(
            String advertisedAddressTest,
            String listeningInterfaceTest,
            int portTest,
            boolean useHostNameAsBookieIdTest,
            boolean useShortHostNameTest,
            boolean allowLoopbackTest,
            Class<? extends Exception> exceptionOutputTest) {

        this.advertisedAddressTest = advertisedAddressTest;
        this.portTest = portTest;
        this.listeningInterfaceTest = listeningInterfaceTest;
        this.useHostNameAsBookieIdTest = useHostNameAsBookieIdTest;
        this.useShortHostNameTest = useShortHostNameTest;
        this.allowLoopbackTest = allowLoopbackTest;
        this.exceptionOutputTest = exceptionOutputTest;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{              //UseHostNameAs          UseShort       Allow
                // Address          Interface    Port       BookieIdTest           HostName       Loopback        ExpectedException
                {"null",            "eth0",      0,         false,                 false,         true,           null},
                {"",                "not_valid", 0,         true,                  false,         false,          java.net.UnknownHostException.class},
                {"192.168.1.100",   null,        1,         true,                  true,          false,          null},
                {"host1",           "",          0,         false,                 true,          true,           IOException.class},
                {"300.300.300.3",   "eth0",      0,         true,                  false,         false,          IOException.class},
                {"",                "eth0",      -1,        false,                 false,         false,          IllegalArgumentException.class},
                {"",                "eth0",      65535,     false,                 false,         true,           null},
                {"",                "eth0",      65534,     true,                  true,          true,           null},
                {"",                "eth0",      65536,     false,                 true,          false,          IllegalArgumentException.class},
                {"",                "eth0",      1,         true,                  false,         true,           null},
                {"",                "eth0",      1,         true,                  true,          false,          java.net.UnknownHostException.class}
        });
    }

    @Before
    public void setup() {
        try {
            setupServer(advertisedAddressTest, portTest, listeningInterfaceTest, useHostNameAsBookieIdTest, useShortHostNameTest, allowLoopbackTest);
        } catch (IOException e) {
            exceptionTest = e.getClass();
        }
    }

    private void setupServer(String advertisedAddressTest, int portTest, String listeningInterfaceTest, boolean useHostNameAsBookieIdTest, boolean useShortHostNameTest, boolean allowLoopbackTest) throws IOException{
        confTest = new ServerConfiguration();
        confTest.setBookiePort(portTest);
        confTest.setListeningInterface(listeningInterfaceTest);
        confTest.setUseHostNameAsBookieID(useHostNameAsBookieIdTest);
        confTest.setUseShortHostName(useShortHostNameTest);
        confTest.setAllowLoopback(allowLoopbackTest);
        if (advertisedAddressTest=="null"){
            confTest.setAdvertisedAddress(null);
        }
        else {
            confTest.setAdvertisedAddress(advertisedAddressTest);
        }
    }

    @Test
    public void test() {
        if (exceptionTest != null){
            Assert.assertEquals(exceptionOutputTest, exceptionTest);
        }
        else {
            try {
                bookieImplTest = spy(BookieImpl.getBookieAddress(confTest));
            } catch (Exception e) {
                Assert.assertEquals(exceptionOutputTest, e.getClass());
            }
        }
    }

}
