package didameetings.server;

import java.util.Enumeration;
import java.util.Hashtable;
import java.util.ArrayList;
import java.util.Collection;

public class RequestHistory {

    private Hashtable<Integer, RequestRecord> pending;
    private Hashtable<Integer, RequestRecord> in_process;
    private Hashtable<Integer, RequestRecord> processed;

    public RequestHistory() {
        this.pending = new Hashtable<Integer, RequestRecord>();
        this.processed = new Hashtable<Integer, RequestRecord>();
        this.in_process = new Hashtable<Integer, RequestRecord>();
    }

    public synchronized RequestRecord getIfPending(int requestid) {
        Integer id = new Integer(requestid);
        return this.pending.get(id);
    }

    public synchronized RequestRecord getFirstPending() {
        Enumeration<Integer> pendingids = this.pending.keys();
        if (pendingids.hasMoreElements()) {
            return this.pending.get(pendingids.nextElement()); 
        }else {
            return null;
        }
    }

    public synchronized Collection<RequestRecord> getAllPending() {
       return new ArrayList<>(this.pending.values());
    }

    public synchronized RequestRecord moveToInProcess(int requestid) {
        Integer id = new Integer(requestid);
        RequestRecord record = this.pending.remove(id);
        this.in_process.put(id, record);
        return record;
    }

    public synchronized RequestRecord getIfProcessed(int requestid) {
        Integer id = new Integer(requestid);
        return this.processed.get(id);
    }
    public synchronized RequestRecord getIfInProcess(int requestid) {
        return this.in_process.get(requestid);
    }
    public synchronized Collection<RequestRecord> getAllInProcess() {
        return new ArrayList<>(this.in_process.values());
    }
    public synchronized RequestRecord getIfExists(int requestid) {
        RequestRecord record;
        Integer id = new Integer(requestid);

        record = this.pending.get(id);
        if (record == null) {
            record = this.processed.get(id);
        }
        return record;
    }

    public synchronized void addToPending(int requestid, RequestRecord record) {
        Integer id = new Integer(requestid);

        this.pending.put(id, record);
    }

    public synchronized RequestRecord moveToProcessed(int requestid) {
        Integer id = new Integer(requestid);
        RequestRecord record = this.in_process.remove(id);
        this.processed.put(id, record);
        return record;
    }
    public synchronized int processedSize() {
        return this.processed.size();
    }

}
