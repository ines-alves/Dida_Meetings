package didameetings.server;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;

import didameetings.DidaMeetingsPaxos;
import didameetings.DidaMeetingsPaxosServiceGrpc;
import didameetings.util.CollectorStreamObserver;
import didameetings.util.GenericResponseCollector;
import didameetings.util.PhaseOneResponseProcessor;
import didameetings.util.PhaseTwoResponseProcessor;
import didameetings.util.PhaseTwoAbortListener;
import io.grpc.ManagedChannel;

public class MainLoop implements Runnable, PhaseTwoAbortListener {

    DidaMeetingsServerState server_state;

    private int has_work;
    private int do_learn;
    private boolean ballot_aborted;
    private int next_log_entry;
    private int lastProcessed;
    private List<Integer> all_participants;
    private int n_participants;
    private String[] targets;
    private ManagedChannel[] channels;
    private DidaMeetingsPaxosServiceGrpc.DidaMeetingsPaxosServiceStub[] async_stubs;

    public MainLoop(DidaMeetingsServerState state) {
        this.server_state = state;
        this.has_work = -1;
        this.next_log_entry = 0;
        this.lastProcessed = 0;
        this.do_learn = -1;
        this.ballot_aborted = false;
    }

    public void run() {
        while (true) {
            while (this.has_work != 0 || (this.server_state.getDebugMode() == 1)) { //debug mode 1 = freeze
                System.out.println(" ---WAITING FOR WORK--- ");
                try {
                    synchronized (this) {
                        checkCrash();
                        wait(); // must hold monitor of `this`
                    }
                } catch (InterruptedException e) {
                }
            }
            checkDelay();
            this.next_log_entry++; 
            System.out.println("--- NEW RUN FOR: " + this.next_log_entry + " ---");
            
            int ballot = this.server_state.getCurrentBallot();
            if ((ballot > -1) && (this.server_state.scheduler.leader(ballot) == this.server_state.my_id)) { //only the leader executes
                //int completed_ballot = this.server_state.getCompletedBallot();
                Map<Integer, PhaseOneResponseProcessor.PhaseOneReplyArgs> phase1Results = phase1(this.next_log_entry);//FIXME what about a new leader that has a bigger log entry but hasnt commited the paxos yet
                int phase_two_value = -1;
                for (Map.Entry<Integer, PhaseOneResponseProcessor.PhaseOneReplyArgs> entry : phase1Results.entrySet()) {
                    System.out.println("Instance: " + entry.getKey() + ", Value: " + entry.getValue().value + ", Valballot: " + entry.getValue().valballot);
                    if (entry.getValue().value == -1) {
                        phase_two_value = this.server_state.req_history.getFirstPending().getId();//FIXME imagine if he proposes a value from pending that is already in the log he hasnt just moved it in to processing
                    } else {
                        phase_two_value = entry.getValue().value;
                    }

                    phase2(entry.getKey(), phase_two_value);
                    this.next_log_entry++;
                    }

                phase2Loop();
            }
            this.has_work = -1; //FIXME necessary ?
        }
    }


    public synchronized void phase2Loop(){
        while (!this.ballot_aborted) {
            for (RequestRecord pending_request : this.server_state.req_history.getAllPending()) {
                int phase_two_value = pending_request.getId();
                System.out.println("PHASE 2 LOOP" + this.next_log_entry + "PHASE 2 VAL:" + phase_two_value);
                phase2(this.next_log_entry , phase_two_value);
                this.next_log_entry++; 
            }
            this.has_work = -1;
            //FIXME Diogo you can do better do a function for this part
            while (this.has_work != 0 || (this.server_state.getDebugMode() == 1)) { //debug mode 1 = freeze
                System.out.println(" ---WAITING FOR WORK--- ");
                try {
                    synchronized (this) {
                        checkCrash();
                        wait(); // must hold monitor of `this`
                    }
                } catch (InterruptedException e) {
                }
            }
 
        }
    }

    public synchronized void wakeup(int code) {// 0 = client request, 2 = new ballot, 3 = debug
        this.has_work = code;
        notifyAll();
    }

    public synchronized void learn() {
        this.do_learn = 0;
        notifyAll();
    }


    @Override
    public void onPhaseTwoAborted(int maxballot, int instance) {
        System.out.println("Phase Two aborted. Max ballot: " + maxballot + ", Instance: " + instance);
        this.ballot_aborted = true;
        if (maxballot > this.server_state.getCurrentBallot()) { //FIXME maybe not needed have to think about it
            this.server_state.setCurrentBallot(maxballot);
        }
        int command = this.server_state.paxos_log.getEntry(instance).command_id;
        this.server_state.req_history.moveToPending(command);
        notifyAll(); // FIXME should it notifyAll here ?
    }

    public void checkDelay() {
        if (this.server_state.getDebugMode() == 4) {
            System.out.println("====================================");
            System.out.println("SLOW MODE ON: APPLYING RANDOM DELAY");
            System.out.println("====================================");
            try {
                Thread.sleep(3000); // Sleep for 3 seconds
                System.out.println("===========================");
                System.out.println("SLOW MODE ON: WAKING UP !!!");
                System.out.println("===========================");
            } catch (InterruptedException e) {
                System.out.println("Thread was interrupted and woke up early!");
            }
        }
    }

    public void checkCrash() {
        if (this.server_state.getDebugMode() == 3) {
            System.out.println("====================================");
            System.out.println("I WILL CRASH NOW BYE BYE, GOOD LUCK!");
            System.out.println("====================================");
            System.exit(1);
        }
    }

    public synchronized Map<Integer, PhaseOneResponseProcessor.PhaseOneReplyArgs> phase1(int entry_number) { 
        int ballot = this.server_state.getCurrentBallot(); 
        int completed_ballot = this.server_state.getCompletedBallot();

        List<Integer> acceptors = this.server_state.scheduler.acceptors(ballot);
        int quorum = this.server_state.scheduler.quorum(ballot);
        int n_acceptors = acceptors.size();

        boolean ballot_aborted = false;
        int phase_one_readballot = -1;

        System.out.println("Going to run paxos phase 1");

        // send phase1
        DidaMeetingsPaxos.PhaseOneRequest.Builder phase_one_request_builder = DidaMeetingsPaxos.PhaseOneRequest.newBuilder();
        phase_one_request_builder.setInstance(entry_number);
        phase_one_request_builder.setRequestballot(ballot);

        DidaMeetingsPaxos.PhaseOneRequest phase_one_request = phase_one_request_builder.build();
        System.out.println("Going to send phase 1" + phase_one_request);

        int low_ballot = Math.max(completed_ballot, 0);
        int high_ballot = ballot;

        PhaseOneResponseProcessor phase_one_processor = new PhaseOneResponseProcessor(this.server_state.scheduler, low_ballot, high_ballot);
        //PhaseOneBogusProcessor phase_one_processor = new PhaseOneBogusProcessor(this.server_state.scheduler, low_ballot, high_ballot);

        ArrayList<DidaMeetingsPaxos.LongPhaseOneReply> phase_one_responses = new ArrayList<DidaMeetingsPaxos.LongPhaseOneReply>();
        GenericResponseCollector<DidaMeetingsPaxos.LongPhaseOneReply> phase_one_collector = new GenericResponseCollector<DidaMeetingsPaxos.LongPhaseOneReply>(phase_one_responses, n_acceptors, phase_one_processor);
        
        //checkDelay(); we used this to test the phase 1 beahavior when the leader is slow and there is a new ballot so there os a promissed false 
        for (int i = 0; i < n_acceptors; i++) {
            CollectorStreamObserver<DidaMeetingsPaxos.LongPhaseOneReply> phase_one_observer = new CollectorStreamObserver<DidaMeetingsPaxos.LongPhaseOneReply>(phase_one_collector);
            this.server_state.async_stubs[acceptors.get(i)].longPhaseone(phase_one_request, phase_one_observer);
        }

        phase_one_collector.waitUntilDone();
        if (phase_one_processor.getPromised() == false) {
            ballot_aborted = true;
            int maxballot = phase_one_processor.getHighballot();
            if (maxballot > this.server_state.getCurrentBallot()) {
                this.server_state.setCurrentBallot(maxballot);
            }
        }
        phase_one_readballot = phase_one_processor.getHighballot();
        System.out.println("Paxos phase 1 ended with aborted = " + ballot_aborted + " and read ballot = " + phase_one_readballot);
        if (ballot_aborted == false) {
            return phase_one_processor.getInstancesMap();
            
        }
        return null;
    }

    public synchronized void phase2(int entry_number, int phase_two_value) { 
        int ballot = this.server_state.getCurrentBallot(); 
        List<Integer> acceptors = this.server_state.scheduler.acceptors(ballot); 
        int n_acceptors = acceptors.size();
        int quorum = this.server_state.scheduler.quorum(ballot);

        System.out.println("Going to run paxos phase 2");

        // send phase2
        DidaMeetingsPaxos.PhaseTwoRequest.Builder phase_two_request = DidaMeetingsPaxos.PhaseTwoRequest.newBuilder();
        phase_two_request.setInstance(entry_number);
        phase_two_request.setRequestballot(ballot);
        phase_two_request.setValue(phase_two_value);

        PhaseTwoResponseProcessor phase_two_processor = new PhaseTwoResponseProcessor(quorum,this);//FIXME added this have to check the logic 

        ArrayList<DidaMeetingsPaxos.PhaseTwoReply> phase_two_responses = new ArrayList<DidaMeetingsPaxos.PhaseTwoReply>();
        GenericResponseCollector<DidaMeetingsPaxos.PhaseTwoReply> phase_two_collector = new GenericResponseCollector<DidaMeetingsPaxos.PhaseTwoReply>(phase_two_responses, n_acceptors, phase_two_processor);
        for (int i = 0; i < n_acceptors; i++) {
            CollectorStreamObserver<DidaMeetingsPaxos.PhaseTwoReply> phase_two_observer = new CollectorStreamObserver<DidaMeetingsPaxos.PhaseTwoReply>(phase_two_collector);
            this.server_state.async_stubs[acceptors.get(i)].phasetwo(phase_two_request.build(), phase_two_observer);
        }
    }

    public synchronized void processEntry(PaxosInstance next_entry) { //FIXME Diogo implement here somthing that checks if the request has already been processed or maybe insede learn so we dont call it multiple times
        //System.out.println(" --- > PENDING REQUESTS:" + this.server_state.req_history.getAllPending());
        //System.out.println(" --- > IN PROCESS REQUESTS:" + this.server_state.req_history.getAllInProcess());
        System.out.println("Log entry with number " + next_entry.instance_nb + " has been decided with command id = " + next_entry.command_id);
        RequestRecord request_record = this.server_state.req_history.getIfInProcess(next_entry.command_id); //FIXME i dont think we still need the while loop here

        while (this.lastProcessed != next_entry.instance_nb - 1) { //debug mode 1 = freeze
            System.out.println("Waiting to process entry " + next_entry.instance_nb + " because last processed is " + this.lastProcessed);
            try {
                checkCrash();
                wait();
            } catch (InterruptedException e) {
            }
        }
        // if I receive the paxos decision before the request
        while (request_record == null || (this.server_state.getDebugMode() == 1)) { //debug mode 1 = freeze
            System.out.println("Record not available!");
            try {
                checkCrash();
                wait();
            } catch (InterruptedException e) {
            }
            request_record = this.server_state.req_history.getIfExists(next_entry.command_id);
        }
        
        // exec request in entry
        // System.out.println("Going to process command with id = " + next_entry.command_id);
        DidaMeetingsCommand command = request_record.getRequest();
        boolean result = false;

        DidaMeetingsAction action = command.getAction();

        // System.out.println("Action  = " + action);
        switch (action) {
            case DidaMeetingsAction.OPEN:
                System.out.println("It is an open request with id = " + command.getMeetingId() + " and max = " + this.server_state.max_participants);
                result = this.server_state.meeting_manager.open(command.getMeetingId(), this.server_state.max_participants);
                break;
            case DidaMeetingsAction.ADD:
                result = this.server_state.meeting_manager.addAndClose(command.getMeetingId(), command.getParticipantId());
                break;
            case DidaMeetingsAction.TOPIC:
                result = this.server_state.meeting_manager.setTopic(command.getMeetingId(), command.getParticipantId(), command.getTopicId());
                break;
            case DidaMeetingsAction.CLOSE:
                result = this.server_state.meeting_manager.close(command.getMeetingId());
                break;
            case DidaMeetingsAction.DUMP:
                this.server_state.meeting_manager.dump();
                result = true;
                break;
            default:
                result = false;
                System.err.println("*** Unknown command ****");
                break;
        }

        // sending response
        System.out.println("Setting response for command with id = " + next_entry.command_id + " with result = " + result);
        request_record.setResponse(result);
        this.server_state.req_history.moveToProcessed(request_record.getId());
        this.lastProcessed = next_entry.instance_nb;
        notifyAll();
    }
}
