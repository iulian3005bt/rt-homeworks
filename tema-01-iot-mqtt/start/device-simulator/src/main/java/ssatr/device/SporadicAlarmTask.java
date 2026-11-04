package ssatr.device;

import java.util.Locale;
import java.util.concurrent.BlockingQueue;

/**
 * TASK SPORADIC: tratează alarmele de temperatură.
 *
 * Evenimentele sosesc în coada alarmEvents (puse de taskul periodic).
 * Intervalul minim între sosiri este MIN_INTERARRIVAL_MS: între două eliberări
 * ale taskului (două alarme publicate) trebuie să treacă cel puțin atât.
 * Intervalul este impus de cod: evenimentele care sosesc mai des sunt
 * respinse, ca să nu fie inundat brokerul.
 */
public class SporadicAlarmTask implements Runnable {

    static final long MIN_INTERARRIVAL_MS = 5000;

    private final String deviceId;
    private final MqttConnection mqtt;
    private final BlockingQueue<AlarmEvent> alarmEvents;

    public SporadicAlarmTask(String deviceId, MqttConnection mqtt, BlockingQueue<AlarmEvent> alarmEvents) {
        this.deviceId = deviceId;
        this.mqtt = mqtt;
        this.alarmEvents = alarmEvents;
    }

    @Override
    public void run() {
        // TODO 2: implementați taskul sporadic:
        //   - așteptați un eveniment: alarmEvents.take()
        //   - dacă de la ultima eliberare au trecut mai puțin de
        //     MIN_INTERARRIVAL_MS, respingeți evenimentul (afișați un mesaj)
        //   - altfel publicați pe Config.alarmTopic(deviceId) mesajul
        //     "deviceId;timestamp;temperatura;prag"
        //     (pragul curent poate fi transmis împreună cu temperatura sau
        //      citit din PeriodicTelemetryTask; alegeți și justificați în raport)
        //
        // Vezi exemplul exemple/01-taskuri/.../SporadicTaskExample.java

        long lastRelease = -1;

        try {
            while (true) {
                AlarmEvent e = alarmEvents.take();

                long sinceLast = e.timestamp() - lastRelease;
                if (lastRelease >= 0 && sinceLast < MIN_INTERARRIVAL_MS) {
                    System.out.printf(Locale.US,
                            "[sporadic] alarmă RESPINSĂ (temp = %.2f): au trecut doar %d ms (minim %d ms)%n",
                            e.temperature(), sinceLast, MIN_INTERARRIVAL_MS);
                    continue;
                }
                lastRelease = e.timestamp();

                String message = deviceId + ";" + e.timestamp() + ";"
                        + String.format(Locale.US, "%.2f", e.temperature()) + ";"
                        + String.format(Locale.US, "%.2f", e.threshold());
                mqtt.publish(Config.alarmTopic(deviceId), message);

                long responseMs = System.currentTimeMillis() - e.timestamp();
                System.out.printf(Locale.US, "[sporadic] ALARMĂ publicată: %.2f > %.2f  (R = %d ms)%n",
                        e.temperature(), e.threshold(), responseMs);
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }
}
