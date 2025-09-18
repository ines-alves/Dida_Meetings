package didameetings.util;

import java.util.ArrayList;

import didameetings.DidaMeetingsPaxos;
import didameetings.configs.ConfigurationScheduler;

public class PhaseOneResponseProcessor extends GenericResponseProcessor<DidaMeetingsPaxos.PhaseOneReply>{
    private ConfigurationScheduler scheduler;
    private boolean promised;
    private int value;
    private int low_ballot;
    private int high_ballot;
    private int responses;

    public PhaseOneResponseProcessor(ConfigurationScheduler s, int l, int h){
        this.scheduler = s;
        this.low_ballot = l;
        this.high_ballot = h;
        this.promised = true;
        this.value = -1;
        this.responses = 0;
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
        //System.out.println("------------");
        //System.out.println("last response2:"+ all_responses);
        this.responses++;
    
        if(last_response.getPromised() == false){
            this.promised = false;
            this.high_ballot = Math.max(this.high_ballot, last_response.getMaxballot());
        } else{
            if(last_response.getValue() != -1 && last_response.getValballot() < this.high_ballot){
                this.low_ballot = last_response.getValballot();
                this.value = last_response.getValue();
            }
        }
        if(this.responses < 2){
            return false;
        }
        return this.promised;
    }
    
}
