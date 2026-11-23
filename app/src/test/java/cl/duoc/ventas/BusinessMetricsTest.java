package cl.duoc.ventas;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BusinessMetricsTest {

    private final MBeanServer server = ManagementFactory.getPlatformMBeanServer();

    @AfterEach
    void unregister() throws Exception {
        var name = new ObjectName(BusinessMetrics.OBJECT_NAME);
        if (server.isRegistered(name)) {
            server.unregisterMBean(name);
        }
    }

    @Test
    void exposesCountersThroughJmx() throws Exception {
        var metrics = new BusinessMetrics();
        metrics.register();
        metrics.processed.addAndGet(7);
        metrics.invalid.addAndGet(2);
        metrics.duplicate.addAndGet(1);
        metrics.late.addAndGet(4);

        var name = new ObjectName(BusinessMetrics.OBJECT_NAME);
        assertEquals(7L, server.getAttribute(name, "Procesadas"));
        assertEquals(2L, server.getAttribute(name, "Invalidas"));
        assertEquals(1L, server.getAttribute(name, "Duplicadas"));
        assertEquals(4L, server.getAttribute(name, "Tardias"));
        assertEquals(7L, server.getAttribute(name, "EnviadasDlq"), "DLQ = invalidos + duplicados + tardios");
    }
}
