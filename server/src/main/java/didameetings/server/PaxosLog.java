package didameetings.server;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Hashtable;

public class PaxosLog {

    private Hashtable<Integer, PaxosInstance> log;

    public PaxosLog() {
        this.log = new Hashtable<Integer, PaxosInstance>();
    }

    public synchronized int length() {
        return this.log.size();
    }
    public synchronized Hashtable<Integer, PaxosInstance> getLog() {
        return this.log;
    }
    public synchronized ArrayList<PaxosInstance> getInstancesFrom(int fromIndex) {
        ArrayList<PaxosInstance> result = new ArrayList<>();

        for (Integer key : log.keySet()) {
            if (key > fromIndex) {
                result.add(log.get(key));
            }
        }
        return result;
    }
    public synchronized ArrayList<PaxosInstance> getUndecidedInstances() {
        ArrayList<PaxosInstance> undecidedInstances = new ArrayList<>();
        for (PaxosInstance entry : log.values()) {
            if (!entry.decided) {
                undecidedInstances.add(entry);
            }
        }
        return undecidedInstances;
    }
    public synchronized ArrayList<PaxosInstance> getDecidedInstances() {
        ArrayList<PaxosInstance> decidedInstances = new ArrayList<PaxosInstance>();

        for (Integer key : log.keySet()) {
            PaxosInstance entry = log.get(key);
            if (entry.decided) {
                decidedInstances.add(entry);
            }
        }
        return decidedInstances;
    }
    public synchronized PaxosInstance getEntry(int position) {
        return this.log.get(position);
    }

    public synchronized PaxosInstance testAndSetEntry(int position) {
        PaxosInstance entry = this.log.get(position);

        if (entry == null) {
            entry = new PaxosInstance(position);
            this.log.put(position, entry);
        }
        return entry;
    }

    public synchronized PaxosInstance testAndSetEntry(int position, int ballot) {
        PaxosInstance entry = this.log.get(position);

        if (entry == null) {
            entry = new PaxosInstance(position, ballot);
            this.log.put(position, entry);
        }
        return entry;
    }
    public synchronized PaxosInstance getInstanceByCommandId(int commandId) {
        for (PaxosInstance instance : log.values()) {
            if (instance.command_id == commandId) {
                return instance;
            }
        }
        return null; 
    }
}
