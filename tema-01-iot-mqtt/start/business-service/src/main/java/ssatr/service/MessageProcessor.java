package ssatr.service;

import java.util.Locale;
import java.util.concurrent.BlockingQueue;

/**
 * Preia mesajele de un anumit tip ("telemetry", "alarm" sau "status") din coada
 * inbox, le salvează și actualizează dashboard-ul.
 *
 * Formatul mesajelor (câmpuri separate prin ";"):
 *   telemetry:  deviceId;timestamp;temperatura
 *   alarm:      deviceId;timestamp;temperatura;prag
 *   status:     deviceId;timestamp;text
 */
public class MessageProcessor implements Runnable {

    private final String kind;
    private final BlockingQueue<String> inbox;
    private final CsvStorage storage;
    private final Dashboard dashboard;

    public MessageProcessor(String kind, BlockingQueue<String> inbox, CsvStorage storage, Dashboard dashboard) {
        this.kind = kind;
        this.inbox = inbox;
        this.storage = storage;
        this.dashboard = dashboard;
    }

    @Override
    public void run() {
        // TODO 4: într-o buclă:
        //   - preluați un mesaj: inbox.take()
        //   - salvați mesajul în fișierul CSV: storage.append(kind, mesaj)
        //     (CsvStorage este gata implementată)
        //   - actualizați dashboard-ul, în funcție de kind
        //   - alarmele și confirmările se afișează imediat în consolă
        //   - un mesaj greșit (format invalid) nu trebuie să oprească thread-ul

        while (true) {
            String message;
            try {
                message = inbox.take().strip();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }

            try {
                process(message);
            } catch (RuntimeException e) {
                System.out.println("[" + kind + "] mesaj invalid ignorat: \"" + message + "\" (" + e.getMessage() + ")");
                storage.append("invalid", message);
            }
        }
    }

    private void process(String message) {
        switch (kind) {
            case "telemetry" -> {
                String[] f = fields(message, 3);
                double temperature = Double.parseDouble(f[2]);
                storage.append(kind, message);
                dashboard.updateTelemetry(f[0], temperature);
            }
            case "alarm" -> {
                String[] f = fields(message, 4);
                double temperature = Double.parseDouble(f[2]);
                double threshold = Double.parseDouble(f[3]);
                storage.append(kind, message);
                dashboard.recordAlarm(f[0]);
                System.out.printf(Locale.US, "[ALARMĂ] %s: temperatura %.2f > prag %.2f%n", f[0], temperature, threshold);
            }
            case "status" -> {
                String[] f = fields(message, 3);
                storage.append(kind, message);
                dashboard.updateStatus(f[0], f[2]);
                System.out.println("[STATUS] " + f[0] + ": " + f[2]);
            }
            default -> throw new IllegalStateException("tip de mesaj necunoscut: " + kind);
        }
    }

    private static String[] fields(String message, int n) {
        String[] f = message.split(";", n);
        if (f.length != n) {
            throw new IllegalArgumentException("trebuie " + n + " câmpuri separate prin ';'");
        }
        if (!f[0].matches("[A-Za-z0-9_-]+")) {
            throw new IllegalArgumentException("deviceId invalid: '" + f[0] + "'");
        }
        Long.parseLong(f[1]);
        return f;
    }
}
