package ssatr.service;

import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

/**
 * Ultima stare cunoscută a fiecărui device: ultima temperatură, momentul
 * ultimului mesaj, numărul de alarme etc.
 *
 * Este actualizată de thread-urile MessageProcessor și citită de CommandConsole,
 * deci accesul trebuie sincronizat.
 */
public class Dashboard {

    // TODO 5: alegeți structura de date pentru starea device-urilor
    //   (de exemplu un Map<String, ...> cu cheia deviceId)
    //   și adăugați metodele de actualizare apelate din MessageProcessor.

    private static class DeviceState {
        Double lastTemperature;
        long lastMessageAt;
        int alarmCount;
        String lastStatus = "-";
    }

    private final Map<String, DeviceState> devices = new TreeMap<>();

    public synchronized void updateTelemetry(String deviceId, double temperature) {
        stateOf(deviceId).lastTemperature = temperature;
    }

    public synchronized void recordAlarm(String deviceId) {
        stateOf(deviceId).alarmCount++;
    }

    public synchronized void updateStatus(String deviceId, String text) {
        stateOf(deviceId).lastStatus = text;
    }

    public synchronized boolean isKnown(String deviceId) {
        return devices.containsKey(deviceId);
    }

    private DeviceState stateOf(String deviceId) {
        DeviceState state = devices.computeIfAbsent(deviceId, id -> new DeviceState());
        state.lastMessageAt = System.currentTimeMillis();
        return state;
    }

    /** Afișează un tabel cu starea tuturor device-urilor. */
    public synchronized void print() {
        // TODO 5: afișați câte un rând pentru fiecare device, de exemplu:
        //   DEVICE     TEMP    ULTIMUL MESAJ   ALARME
        //   device-1   23.41   acum 1 s        2

        if (devices.isEmpty()) {
            System.out.println("(niciun device nu a trimis încă date)");
            return;
        }
        long now = System.currentTimeMillis();
        System.out.printf("%-14s %8s %15s %7s  %s%n", "DEVICE", "TEMP", "ULTIMUL MESAJ", "ALARME", "ULTIMUL STATUS");
        for (Map.Entry<String, DeviceState> entry : devices.entrySet()) {
            DeviceState s = entry.getValue();
            String temp = s.lastTemperature == null ? "-" : String.format(Locale.US, "%.2f", s.lastTemperature);
            String age = "acum " + (now - s.lastMessageAt) / 1000 + " s";
            System.out.printf("%-14s %8s %15s %7d  %s%n", entry.getKey(), temp, age, s.alarmCount, s.lastStatus);
        }
    }
}
