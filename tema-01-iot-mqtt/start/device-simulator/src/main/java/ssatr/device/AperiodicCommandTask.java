package ssatr.device;

import java.util.Locale;
import java.util.concurrent.BlockingQueue;

/**
 * TASK APERIODIC: execută comenzile primite de la business service.
 *
 * Comenzile sosesc oricând, pe MQTT, fără un interval minim între ele, și sunt
 * puse în coada commands. Fiecare comandă eliberează un job al taskului.
 * Comenzi care trebuie suportate:
 *
 *   SET_INTERVAL <ms>       schimbă perioada T a taskului periodic
 *   SET_THRESHOLD <valoare> schimbă pragul de alarmă
 *   RESET                   readuce senzorul la valoarea inițială
 *   STATUS                  raportează perioada și pragul curente
 *
 * După executarea fiecărei comenzi, device-ul publică pe Config.statusTopic(deviceId)
 * o confirmare: "deviceId;timestamp;text", de exemplu
 * "device-1;1727600000000;OK SET_INTERVAL 1000".
 */
public class AperiodicCommandTask implements Runnable {

    private static final long MIN_PERIOD_MS = 100;
    private static final long MAX_PERIOD_MS = 60_000;

    private final String deviceId;
    private final TemperatureSensor sensor;
    private final MqttConnection mqtt;
    private final BlockingQueue<String> commands;
    private final PeriodicTelemetryTask periodic;

    public AperiodicCommandTask(String deviceId, TemperatureSensor sensor, MqttConnection mqtt,
                                BlockingQueue<String> commands, PeriodicTelemetryTask periodic) {
        this.deviceId = deviceId;
        this.sensor = sensor;
        this.mqtt = mqtt;
        this.commands = commands;
        this.periodic = periodic;
    }

    @Override
    public void run() {
        // TODO 3: implementați taskul aperiodic:
        //   - preluați comenzile din coadă: commands.take()
        //   - separați numele comenzii de parametru: command.split(" ")
        //   - executați comanda (periodic.setPeriodMs(...), sensor.reset() etc.)
        //   - publicați confirmarea pe Config.statusTopic(deviceId)
        //   - o comandă necunoscută sau un parametru invalid nu trebuie să
        //     oprească taskul: publicați "ERROR <motiv>"
        //
        // Vezi exemplul exemple/01-taskuri/.../AperiodicTaskExample.java

        try {
            while (true) {
                String command = commands.take().trim();

                String reply;
                try {
                    reply = execute(command);
                } catch (IllegalArgumentException e) {
                    reply = "ERROR " + e.getMessage();
                } catch (RuntimeException e) {
                    reply = "ERROR " + e;
                }
                reply = safeText(reply);

                mqtt.publish(Config.statusTopic(deviceId), deviceId + ";" + System.currentTimeMillis() + ";" + reply);
                System.out.println("[aperiodic] " + command + " -> " + reply);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private String execute(String command) {
        if (command.isEmpty()) {
            throw new IllegalArgumentException("comanda goala");
        }
        String[] parts = command.split("\\s+");
        String name = parts[0].toUpperCase(Locale.ROOT);

        switch (name) {
            case "SET_INTERVAL" -> {
                requireArgs(parts, 1, "SET_INTERVAL <ms>");
                long ms = parseLong(parts[1]);
                if (ms < MIN_PERIOD_MS || ms > MAX_PERIOD_MS) {
                    throw new IllegalArgumentException(
                            "interval in afara domeniului " + MIN_PERIOD_MS + ".." + MAX_PERIOD_MS + " ms: " + ms);
                }
                periodic.setPeriodMs(ms);
                return "OK SET_INTERVAL " + ms;
            }
            case "SET_THRESHOLD" -> {
                requireArgs(parts, 1, "SET_THRESHOLD <valoare>");
                double value = parseDouble(parts[1]);
                periodic.setThreshold(value);
                return "OK SET_THRESHOLD " + String.format(Locale.US, "%.2f", value);
            }
            case "RESET" -> {
                requireArgs(parts, 0, "RESET");
                sensor.reset();
                return "OK RESET";
            }
            case "STATUS" -> {
                requireArgs(parts, 0, "STATUS");
                return "OK STATUS interval=" + periodic.getPeriodMs()
                        + " threshold=" + String.format(Locale.US, "%.2f", periodic.getThreshold());
            }
            default -> throw new IllegalArgumentException("comanda necunoscuta: " + parts[0]);
        }
    }

    private static void requireArgs(String[] parts, int count, String usage) {
        if (parts.length - 1 != count) {
            throw new IllegalArgumentException("utilizare: " + usage);
        }
    }

    private static long parseLong(String text) {
        try {
            return Long.parseLong(text);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("parametru invalid: " + text);
        }
    }

    private static double parseDouble(String text) {
        try {
            double value = Double.parseDouble(text);
            if (Double.isNaN(value) || Double.isInfinite(value)) {
                throw new NumberFormatException();
            }
            return value;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("parametru invalid: " + text);
        }
    }

    private static String safeText(String text) {
        return text.replaceAll("[^\\x20-\\x7E]|;", "?");
    }
}
