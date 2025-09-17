package didameetings.util;

import java.util.ArrayList;

import didameetings.DidaMeetingsPaxos;
import didameetings.DidaMeetingsPaxos.PhaseOneReply;
import didameetings.configs.ConfigurationScheduler;

public class PhaseOneResponseProcessor extends GenericResponseProcessor<DidaMeetingsPaxos.PhaseOneReply>{
    private ConfigurationScheduler scheduler;
    private boolean promised;
    private int value;
    private int low_ballot;
    private int high_ballot;

    public PhaseOneResponseProcessor(ConfigurationScheduler s, int l, int h){
        this.scheduler = s;
        this.low_ballot = l;
        this.high_ballot = h;
        this.promised = true;
        this.value = -1;
    }
    public boolean getPromised() {
        return this.promised;
    }

    public int getValue() {
        return this.value;
    }

    public int getLowballot() {
        return this.low_ballot;
    }

    public int getHighballot() {
        return this.high_ballot;
    }
    @Override
    public synchronized boolean onNext(ArrayList<DidaMeetingsPaxos.PhaseOneReply> all_responses, DidaMeetingsPaxos.PhaseOneReply last_response) {
        System.out.println("all reponses size: " + all_responses.size());
        
        for(PhaseOneReply reply : all_responses){
            if(reply.getPromised() == false){
                this.promised = false;
                this.high_ballot = reply.getMaxballot();  
            }else{
                if(reply.getValballot() > this.low_ballot){
                    this.low_ballot = reply.getValballot();
                    this.value = reply.getValue();
                }
            }
        }
        return this.promised;
    }
    
}
