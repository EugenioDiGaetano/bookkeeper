package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collection;

import static org.mockito.Mockito.*;


@RunWith(Parameterized.class)
public class BookieImplgetAddressTest {
    private static final int DEFAULT_PORT = 3181;
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
        return Arrays.asList(new Object[][]{                            //UseHostNameAs          UseShort       Allow
                // Address          Interface       Port                  BookieIdTest           HostName       Loopback        ExpectedException
                {"null",            "default",      0,                    false,                 false,         true,           null},
                {"",                "not_valid",    0,                    true,                  false,         false,          java.net.UnknownHostException.class},
                {"192.168.1.100",   "null",         1,                    true,                  true,          false,          null},
                {"host1",           "",             0,                    false,                 true,          true,           IOException.class},
                {"300.300.300.3",   "default",      0,                    true,                  false,         false,          IOException.class},
                {"",                "default",      -1,                   false,                 false,         false,          IllegalArgumentException.class},
                {"",                "default",      65535,                false,                 false,         true,           null},
                {"",                "default",      65534,                true,                  true,          true,           null},
                {"",                "default",      65536,                false,                 true,          false,          IllegalArgumentException.class},
                {"",                "default",      Integer.MIN_VALUE,    true,                  false,         true,           null},
                {"",                "default",      1,                    true,                  true,          false,          java.net.UnknownHostException.class}
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
        confTest.setUseHostNameAsBookieID(useHostNameAsBookieIdTest);
        confTest.setUseShortHostName(useShortHostNameTest);
        confTest.setAllowLoopback(allowLoopbackTest);

        if (portTest!=-Integer.MIN_VALUE){
            confTest.setBookiePort(portTest);
        }

        if (advertisedAddressTest=="null"){
            confTest.setAdvertisedAddress(null);
        }
        else {
            confTest.setAdvertisedAddress(advertisedAddressTest);
        }

        if (listeningInterfaceTest=="null"){
            confTest.setListeningInterface(null);
        }
        else {
            confTest.setListeningInterface(listeningInterfaceTest);
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
                if (portTest != -Integer.MIN_VALUE) {
                    Assert.assertEquals("Porta attesa: " + portTest, portTest, bookieImplTest.getPort());
                } else {
                    Assert.assertEquals("Porta di default attesa: " + DEFAULT_PORT, DEFAULT_PORT, bookieImplTest.getPort());
                }
                if (advertisedAddressTest != "" && advertisedAddressTest != "null") {
                    // Se l'indirizzo pubblicizzato non è "null", vuoto o "null" come stringa, verifica che corrisponda
                    Assert.assertEquals("Indirizzo pubblicizzato atteso: " + advertisedAddressTest, advertisedAddressTest, bookieImplTest.getHostName());
                } else if (useHostNameAsBookieIdTest) {
                    // Se si usa l'hostname come ID Bookie, verifica l'indirizzo dell'host
                    String hostName = useShortHostNameTest ?
                            InetAddress.getLocalHost().getCanonicalHostName().split("\\.", 2)[0] :
                            InetAddress.getLocalHost().getCanonicalHostName();
                    Assert.assertEquals("Nome host atteso (usando hostname come ID Bookie): " + hostName, hostName, bookieImplTest.getHostName());
                } else {
                    // Altrimenti, se l'indirizzo è "null" o vuoto, verifica l'indirizzo IP locale
                    String expectedHost = (advertisedAddressTest == "null" || advertisedAddressTest == "") ?
                            InetAddress.getLocalHost().getHostAddress() : advertisedAddressTest;
                    Assert.assertEquals("Nome host atteso (indirizzo pubblicizzato nullo o vuoto): " + expectedHost, expectedHost, bookieImplTest.getHostName());
                }
            } catch (Exception e) {
                Assert.assertEquals("Eccezione attesa: " + exceptionOutputTest.getName(), exceptionOutputTest, e.getClass());
            }
        }
    }

}
