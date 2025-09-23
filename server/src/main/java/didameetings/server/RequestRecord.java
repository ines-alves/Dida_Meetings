package didameetings.server;

public class RequestRecord {

    private int requestid;
    private DidaMeetingsCommand request;
    private boolean response_available;
    private boolean response_value;
    private boolean isDecided;

    public RequestRecord(int id) {
        this.requestid = id;
        this.request = null;
        this.response_available = false;
        this.response_value = false;
        this.isDecided = false;
    }

    public RequestRecord(int id, DidaMeetingsCommand rq) {
        this.requestid = id;
        this.request = rq;
        this.response_available = false;
        this.response_value = false;
        this.isDecided = false;
    }

    // Getter and Setter methods for all fields
    public DidaMeetingsCommand getRequest() {
        return this.request;
    }

    public int getId() {
        return this.requestid;
    }

    public void setRequest(DidaMeetingsCommand rq) {
        this.request = rq;
    }

    public boolean getResponseValue() {
        return this.response_value;
    }

    public synchronized void setResponse(boolean resp) {
        this.response_value = resp;
        this.response_available = true;
        this.notifyAll();
    }
    public boolean getIsDecided() {
        return this.isDecided;
    }
    public void setIsDecided(boolean d) {
        this.isDecided = d;
    }
    public synchronized boolean waitForResponse() {
        while (this.response_available == false) {
            try {
                wait();
            } catch (InterruptedException e) {
            }
        }
        return this.response_value;
    }
}
