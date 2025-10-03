package didameetings.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import didameetings.DidaMeetingsPaxos;
import didameetings.configs.ConfigurationScheduler;

public class PhaseOneResponseProcessor extends GenericResponseProcessor<DidaMeetingsPaxos.LongPhaseOneReply>{

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
    private Map<Integer, PhaseOneReplyArgs> instancesMap;
    private int lowBallot;
    private int highBallot;
    private  boolean promised;

    public PhaseOneResponseProcessor(ConfigurationScheduler s, int l, int h){
        this.scheduler = s;
        this.instancesMap = new HashMap<>();
        this.promised = true;
        this.lowBallot = l;
        this.highBallot = h;
        
    }
    public boolean getPromised() {
        return this.promised;
    }

    public int getValue(int instance) {
        PhaseOneReplyArgs arg = instancesMap.get(instance);
        return  arg.value;
    }

    public int getValballot(int instance) {
        PhaseOneReplyArgs arg = instancesMap.get(instance);
        return  arg.valballot;
    }

    public int getLowballot() {
        return this.lowBallot;
    }
    public int getHighballot() {
        return this.highBallot;
    }
    public Map<Integer, PhaseOneReplyArgs> getInstancesMap() {
        return this.instancesMap;
    }
    @Override
    public synchronized boolean onNext(ArrayList<DidaMeetingsPaxos.LongPhaseOneReply> all_responses, DidaMeetingsPaxos.LongPhaseOneReply last_response) {
        List<DidaMeetingsPaxos.PhaseOneReply> replies = last_response.getLongPhaseOneList();

        for (DidaMeetingsPaxos.PhaseOneReply reply : replies) {
            if (!reply.getPromised()) {
                this.promised = false;
                this.highBallot = Math.max(this.highBallot, reply.getMaxballot());
                break;
            } else if (this.instancesMap.get(reply.getInstance()) == null || reply.getValballot() > this.instancesMap.get(reply.getInstance()).valballot) {
                // If it's the first time we see this instance or if this reply has a higher valballot than the stored one
                PhaseOneReplyArgs args = new PhaseOneReplyArgs(reply.getValue(), reply.getValballot());
                this.instancesMap.put(reply.getInstance(), args);
            } 
                
        }
        if(all_responses.size() < 2) {
            return false;
        } else {
            return true;
        }
    }

}
    

