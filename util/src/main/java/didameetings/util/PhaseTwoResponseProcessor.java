package didameetings.util;

import java.util.ArrayList;

import didameetings.DidaMeetingsPaxos;

public class PhaseTwoResponseProcessor extends GenericResponseProcessor<DidaMeetingsPaxos.PhaseTwoReply> {

    private boolean accepted;
    private int maxballot;
    private int quorum;
    private int responses;
    private PhaseTwoListener listener;

    public PhaseTwoResponseProcessor(int q, PhaseTwoListener listener) {
        this.accepted = true;
        this.maxballot = 0;
        this.quorum = q;
        this.listener = listener;
        this.responses = 0;
    }

    public boolean getAccepted() {
        return this.accepted;
    }

    public int getMaxballot() {
        return this.maxballot;
    }

    public synchronized boolean onNext(ArrayList<DidaMeetingsPaxos.PhaseTwoReply> all_responses, DidaMeetingsPaxos.PhaseTwoReply last_response) {
        this.responses++;
        if (last_response.getAccepted() == false) {
            System.out.println(" --- PHASE TWO REJECTED --- ");//FIXME this is never hapening
            this.accepted = false;
            if (last_response.getMaxballot() > this.maxballot) {
                this.maxballot = last_response.getMaxballot();
            }
            listener.onPhaseTwoAborted(this.maxballot,last_response.getInstance());
            return true;
        } else if (responses >= quorum) {
            listener.onPhaseTwoFinish();
            return true;
        }else {
            return false;
        }
    }
}
