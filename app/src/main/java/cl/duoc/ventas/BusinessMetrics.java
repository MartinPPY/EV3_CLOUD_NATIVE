package cl.duoc.ventas;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicLong;

/** Contadores de negocio (procesadas, inválidas, duplicadas, tardías): logs, tests y JMX. */
public final class BusinessMetrics implements BusinessMetricsMBean {

    static final String OBJECT_NAME = "cl.duoc.ventas:type=BusinessMetrics";

    public final AtomicLong processed = new AtomicLong();
    public final AtomicLong invalid = new AtomicLong();
    public final AtomicLong duplicate = new AtomicLong();
    public final AtomicLong late = new AtomicLong();

    /** Solo la app real lo llama: los tests crean varias instancias y el ObjectName es único. */
    public void register() {
        try {
            MBeanServer server = ManagementFactory.getPlatformMBeanServer();
            server.registerMBean(this, new ObjectName(OBJECT_NAME));
        } catch (JMException e) {
            throw new IllegalStateException("No se pudo registrar el MBean " + OBJECT_NAME, e);
        }
    }

    @Override
    public long getProcesadas() {
        return processed.get();
    }

    @Override
    public long getInvalidas() {
        return invalid.get();
    }

    @Override
    public long getDuplicadas() {
        return duplicate.get();
    }

    @Override
    public long getTardias() {
        return late.get();
    }

    @Override
    public long getEnviadasDlq() {
        return invalid.get() + duplicate.get() + late.get();
    }

    public String summary() {
        return "procesadas=" + processed.get() + " invalidos=" + invalid.get()
                + " duplicados=" + duplicate.get() + " tardios=" + late.get();
    }
}
