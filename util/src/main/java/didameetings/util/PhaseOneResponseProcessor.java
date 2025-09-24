package didameetings.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import didameetings.DidaMeetingsPaxos;
import didameetings.configs.ConfigurationScheduler;

public class PhaseOneResponseProcessor extends GenericResponseProcessor<DidaMeetingsPaxos.LongPhaseOneReply>{
    /*
     * 
     private ConfigurationScheduler scheduler;
     private boolean promised;
     private int value;
     private int low_ballot;
     private int high_ballot;
     private int responses;
     */
    public static class PhaseOneReplyArgs {
        public int value;
        public int valballot;
        public int maxballot;


        public PhaseOneReplyArgs(int value, int l) {
            this.value = value;
            this.valballot = l;
        }

        
    }
    private ConfigurationScheduler scheduler;
    private Map<Integer, PhaseOneReplyArgs> undecidedMap;
    private int lowBallot;
    private int highBallot;
    private  boolean promised;
    private int maxInstancenum;

    public PhaseOneResponseProcessor(ConfigurationScheduler s, int l, int h){
        this.scheduler = s;
        this.undecidedMap = new HashMap<>();
        this.promised = true;
        this.lowBallot = l;
        this.highBallot = h;
        
    }
    public boolean getPromised() {
        return this.promised;
    }

    public int getValue(int instance) {
        PhaseOneReplyArgs arg = undecidedMap.get(instance);
        return  arg.value;
    }

    public int getValballot(int instance) {
        PhaseOneReplyArgs arg = undecidedMap.get(instance);
        return  arg.valballot;
    }

    public int getLowballot() {
        return this.lowBallot;
    }
    public int getHighballot() {
        return this.highBallot;
    }
    public Map<Integer, PhaseOneReplyArgs> getUndecidedMap() {
        return this.undecidedMap;
    }
    @Override
     public synchronized boolean onNext(ArrayList<DidaMeetingsPaxos.LongPhaseOneReply> all_responses, DidaMeetingsPaxos.LongPhaseOneReply last_response) {
        List<DidaMeetingsPaxos.PhaseOneReply> replies = last_response.getLongPhaseOneList();

        for (DidaMeetingsPaxos.PhaseOneReply reply : replies) {
            if (!reply.getPromised()) {
                this.promised = false;
                this.highBallot = Math.max(this.highBallot, reply.getMaxballot());
                break;
            } else {
                PhaseOneReplyArgs args = new PhaseOneReplyArgs(reply.getValue(), reply.getValballot());
                if (reply.getValue() != -1 && reply.getValballot() < this.highBallot) {
                    this.lowBallot = reply.getValballot();
                }
                this.undecidedMap.put(reply.getInstance(), args);
            }
        }

        if (all_responses.size() < 2) {
            return false;
        }
        return this.promised;
    }
    
}
