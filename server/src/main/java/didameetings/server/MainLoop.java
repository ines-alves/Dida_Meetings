package didameetings.server;

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
import io.grpc.ManagedChannel;

public class MainLoop implements Runnable {

    DidaMeetingsServerState server_state;

    private int has_work;
    private int next_log_entry;
    private List<Integer> all_participants;
    private int n_participants;
    private String[] targets;
    private ManagedChannel[] channels;
    private DidaMeetingsPaxosServiceGrpc.DidaMeetingsPaxosServiceStub[] async_stubs;

    public MainLoop(DidaMeetingsServerState state) {
        this.server_state = state;
        this.has_work = -1;
        this.next_log_entry = 0;
    }

    public void run() {
        while (true) {
            /*
            checkCrash();
            checkDelay();
            while (this.server_state.getDebugMode() == 1) {
                System.out.println("===============================");
                System.out.println("INSIDE DEBUG MODE 1 AKA FREEZE");
                System.out.println("===============================");
                try {
                    synchronized (this) {
                        wait(); // must hold monitor of `this`
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt(); // restore interrupt flag
                }
            }
            */

            while (this.has_work != 0 || (this.server_state.getDebugMode() == 1)) { //FIXME check this later
                System.out.println(" ---WAITING FOR WORK--- ");
                try {
                    //checkCrash();
                    //wait();
                    synchronized (this) {
                        wait(); // must hold monitor of `this`
                    }
                } catch (InterruptedException e) {
                }
            }

            
            //PaxosInstance next_entry = this.server_state.paxos_log.testAndSetEntry(this.next_log_entry);
            this.next_log_entry++; // PPERIGO AQUI ESTAVA EM PHASE 1
            System.out.println("--- NEW THREAD FOR: " + this.next_log_entry + " ---");
    
            new Thread(() -> {
                      multiPaxos(this.next_log_entry);   
                        }).start();
            this.has_work = -1;
            /*
            while (next_entry.decided == false) {
                while (this.has_work == -1 || (this.server_state.getDebugMode() == 1)) { //FIXME check this later
                    System.out.println(" ---HERE 1--- ");
                    try {
                        //checkCrash();
                        //wait();
                        synchronized (this) {
                            wait(); // must hold monitor of `this`
                        }
                    } catch (InterruptedException e) {
                    }
                }
                System.out.println(" ---HERE 2--- ");
                int ballot = this.server_state.getCurrentBallot();
                RequestRecord request_record = this.server_state.req_history.getFirstPending();

                if ((ballot > -1) && (request_record != null) && (this.server_state.scheduler.leader(ballot) == this.server_state.my_id)) { //only the leader executes
                    System.out.println("I am the leader for request with id " + request_record.getId());
                    int completed_ballot = this.server_state.getCompletedBallot();
                    System.out.println("This is the server current ballot = " + ballot + " and this is the completed ballot = " + completed_ballot);
                    
                    if (ballot > completed_ballot) {
                        this.longPhase1(this.next_log_entry);
                    } else{
                        int phase_two_value = request_record.getId();
                        this.phase2(this.next_log_entry, phase_two_value);
                    }
                } 
                this.has_work = false;
     
            }
            processEntry(next_entry);
            //this.next_log_entry++;
            //this.processEntry(this.next_log_entry);
            */
        }
    }

    public synchronized void wakeup(int code) {// 0 = client request, 1 = learn, 2 = new ballot, 3 = debug
        this.has_work = code;
        notify();
    }

    private long randomDelay() {
        Random random = new Random();
        return random.nextInt(3000); // Sleep for a random duration up to 3 seconds
    }

    public void checkDelay() {
        if (this.server_state.getDebugMode() == 4) {
            System.out.println("====================================");
            System.out.println("SLOW MODE ON: APPLYING RANDOM DELAY");
            System.out.println("====================================");
            try {
                Thread.sleep(10000); // Sleep for a random duration
                System.out.println("====================================");
                System.out.println("SLOW MODE ON: WAKING UP !!!");
                System.out.println("====================================");
                //Thread.sleep(randomDelay()); // Sleep for a random duration
            } catch (InterruptedException e) {
                System.out.println("Thread was interrupted and woke up early!");
                //Thread.currentThread().interrupt(); // Restore interrupt flag
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
    /*
     * 
     public synchronized void multiPaxos(PaxosInstance entry) {
         ArrayList<Integer> undecided_instances = this.server_state.paxos_log.getUndecidedInstances();
         
         int ballot = this.server_state.getCurrentBallot();
         if ((ballot > -1) && (this.server_state.scheduler.leader(ballot) == this.server_state.my_id)) {
            
             int completed_ballot = this.server_state.getCompletedBallot();
             if (ballot > completed_ballot) {
                 for(Integer undecided : undecided_instances){
                     new Thread(() -> {
                         phase1(undecided,-1);   
                     });
                     
                 }
             } else{
                 for (RequestRecord request_record : this.server_state.req_history.getAllPending()) {
                     new Thread(() -> phase2(this.next_log_entry, request_record.getId())).start();
                 }
             }
             
             
             
             processEntry(entry);
         }
     }
     */
  
     public synchronized void multiPaxos(int entry_number) {
        System.out.println("ENTROU MULTIPAXOS");
        PaxosInstance next_entry = this.server_state.paxos_log.testAndSetEntry(entry_number);

        int ballot = this.server_state.getCurrentBallot();
        RequestRecord request_record = this.server_state.req_history.getFirstPending();
        if ((ballot > -1) && (request_record != null) && (this.server_state.scheduler.leader(ballot) == this.server_state.my_id)) { //only the leader executes
            System.out.println("I am the leader for request with id " + request_record.getId());
            int completed_ballot = this.server_state.getCompletedBallot();
            //System.out.println("This is the server current ballot = " + ballot + " and this is the completed ballot = " + completed_ballot);
            
            if (ballot > completed_ballot) {
                phase1(next_entry.instance_nb);        
            }else{
                System.out.println("---UNDECIDED LIST:" + this.server_state.paxos_log.getUndecidedInstances());
                int entryNum = this.server_state.paxos_log.length() - 1;
                for (RequestRecord pending_request : this.server_state.req_history.getAllPending()) {
                    int phase_two_value = pending_request.getId();
                    int curEntryNum = entryNum++;
                    System.out.println("CURRENT ENTRY NUM:" + curEntryNum + "PHASE 2 VAL:" +phase_two_value);
                    //this.server_state.req_history.addToInProcess(phase_two_value, pending_request);
                    new Thread(() -> phase2(curEntryNum, phase_two_value)).start();
                    
                }
            }
                
        } 
        
        while (this.has_work != 1 || (this.server_state.getDebugMode() == 1)) { //FIXME check this later this is for the none leader nodes
                System.out.println(" ---WAITING FOR ACCEPT--- ");
                try {
                    //checkCrash();
                    //wait();
                    synchronized (this) {
                        wait(); // must hold monitor of `this`
                    }
                } catch (InterruptedException e) {
                }
        }
        Collection<RequestRecord> inProcess_requests = this.server_state.req_history.getAllInProcess();
        for(RequestRecord inProcess_request : inProcess_requests){
            int numProcessed = this.server_state.req_history.processedSize() - 1;
            PaxosInstance request_entry = this.server_state.paxos_log.getInstanceByCommandId(inProcess_request.getId());
            if(inProcess_request.getIsDecided() == true && (this.server_state.paxos_log.testAndSetEntry(numProcessed).instance_nb <= request_entry.instance_nb)){
                System.out.println(" ---Processing instance--- " + inProcess_request.getId());
                processEntry(request_entry);
                
            }
        }
        this.has_work = -1;
     }
     
        

    /*
     
     public synchronized void longPhase1(int entry_number) { //later this shall not be synchronized
         PaxosInstance next_entry = this.server_state.paxos_log.testAndSetEntry(entry_number);
         for (RequestRecord request_record : this.server_state.req_history.getAllPending()) {
             new Thread(() -> phase1(entry_number, request_record.getId())).start();
         }
     }
     */

    public synchronized void phase1(int entry_number) { //later this shall not be synchronized
        int ballot = this.server_state.getCurrentBallot();  // repeated code FIXME
        int completed_ballot = this.server_state.getCompletedBallot();// repeated code FIXME

        List<Integer> acceptors = this.server_state.scheduler.acceptors(ballot);
        int quorum = this.server_state.scheduler.quorum(ballot);
        int n_acceptors = acceptors.size();
        
        boolean ballot_aborted = false;
        int phase_one_readballot = -1;
        
        

        // Paxos Phase One
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

        for (int i = 0; i < n_acceptors; i++) {
            CollectorStreamObserver<DidaMeetingsPaxos.LongPhaseOneReply> phase_one_observer = new CollectorStreamObserver<DidaMeetingsPaxos.LongPhaseOneReply>(phase_one_collector);
            this.server_state.async_stubs[acceptors.get(i)].longPhaseone(phase_one_request, phase_one_observer);
        }

        checkDelay();
        phase_one_collector.waitUntilDone();
        if (phase_one_processor.getPromised() == false) {
            ballot_aborted = true;
            int maxballot = phase_one_processor.getHighballot();
            if (maxballot > this.server_state.getCurrentBallot()) {
                this.server_state.setCurrentBallot(maxballot);
            }
        }
        System.out.println("UNDECIDED LIST:" + this.server_state.paxos_log.getUndecidedInstances());
        phase_one_readballot = phase_one_processor.getHighballot();
        System.out.println("Paxos phase 1 ended with aborted = " + ballot_aborted + " and read ballot = " + phase_one_readballot);
       
        if (ballot_aborted == false) {
            Map<Integer, PhaseOneResponseProcessor.PhaseOneReplyArgs> undecidedMap = phase_one_processor.getUndecidedMap();
            System.out.println("SIZE UNDECIDE INSTANCES: "+ undecidedMap.size());
            int pendingIndex = 0;
            for(Integer instance : undecidedMap.keySet()){
                int phase_two_value; 
                PhaseOneResponseProcessor.PhaseOneReplyArgs args = undecidedMap.get(instance);
                if (args.value == -1) {
                    phase_two_value = this.server_state.req_history.getPendingAtIndex(pendingIndex).getId(); 
                    pendingIndex++;
                    //this.server_state.req_history.addToInProcess(phase_two_value, this.server_state.req_history.getIfPending(phase_two_value));
                } else {
                    phase_two_value = args.value;
                }
                System.out.println("Starting phase 2 for instance " + instance + " with value " + phase_two_value);
                new Thread(() -> phase2(instance, phase_two_value)).start();          
            }
        }
    
    }

    public synchronized void phase2(int entry_number ,int phase_two_value) { //later this shall not be synchronized
        int ballot = this.server_state.getCurrentBallot();  // repeated code FIXME
        List<Integer> acceptors = this.server_state.scheduler.acceptors(ballot); // repeated code FIXME
        int n_acceptors = acceptors.size();
        boolean ballot_aborted = false;
        int quorum = this.server_state.scheduler.quorum(ballot);

        PaxosInstance next_entry = this.server_state.paxos_log.testAndSetEntry(entry_number);

        System.out.println("Going to run paxos phase 2");

        // send phase2
        DidaMeetingsPaxos.PhaseTwoRequest.Builder phase_two_request = DidaMeetingsPaxos.PhaseTwoRequest.newBuilder();
        phase_two_request.setInstance(entry_number);
        phase_two_request.setRequestballot(ballot);
        phase_two_request.setValue(phase_two_value);

        PhaseTwoResponseProcessor phase_two_processor = new PhaseTwoResponseProcessor(quorum);

        //System.out.println("Calling peers with phase_two_request = " + phase_two_request);
        ArrayList<DidaMeetingsPaxos.PhaseTwoReply> phase_two_responses = new ArrayList<DidaMeetingsPaxos.PhaseTwoReply>();
        GenericResponseCollector<DidaMeetingsPaxos.PhaseTwoReply> phase_two_collector = new GenericResponseCollector<DidaMeetingsPaxos.PhaseTwoReply>(phase_two_responses, n_acceptors, phase_two_processor);
        for (int i = 0; i < n_acceptors; i++) {
            CollectorStreamObserver<DidaMeetingsPaxos.PhaseTwoReply> phase_two_observer = new CollectorStreamObserver<DidaMeetingsPaxos.PhaseTwoReply>(phase_two_collector);
            this.server_state.async_stubs[acceptors.get(i)].phasetwo(phase_two_request.build(), phase_two_observer);
        }

        System.out.println("Waiting for responses...");
        phase_two_collector.waitUntilDone();
        if (phase_two_processor.getAccepted() == false) {
            ballot_aborted = true;
            this.server_state.setCurrentBallot(phase_two_processor.getMaxballot());
        }
        System.out.println("Paxos phase 2 ended with ballot_aborted = " + ballot_aborted);
    

        // After phase2
        if (ballot_aborted == false) {
            this.server_state.setCompletedBallot(ballot);
            next_entry.command_id = phase_two_value;
            next_entry.decided = true;
        }
    }

    public synchronized void processEntry(PaxosInstance next_entry) {
        /*
        PaxosInstance next_entry = this.server_state.paxos_log.testAndSetEntry(entry_number);

        while (next_entry.decided == false) {
            RequestRecord request_record = this.server_state.req_history.getFirstPending();
            int ballot = this.server_state.getCurrentBallot();
            int completed_ballot = this.server_state.getCompletedBallot();

            List<Integer> acceptors = this.server_state.scheduler.acceptors(ballot);
            int quorum = this.server_state.scheduler.quorum(ballot);
            int n_acceptors = acceptors.size();

            if ((ballot > -1) && (request_record != null) && (this.server_state.scheduler.leader(ballot) == this.server_state.my_id)) {

                System.out.println("I am the leader for request with id " + request_record.getId());
                boolean ballot_aborted = false;
                int phase_one_readballot = -1;
                int phase_two_value = request_record.getId();

                // Paxos Phase One
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

                for (int i = 0; i < n_acceptors; i++) {
                    CollectorStreamObserver<DidaMeetingsPaxos.LongPhaseOneReply> phase_one_observer = new CollectorStreamObserver<DidaMeetingsPaxos.LongPhaseOneReply>(phase_one_collector);
                    this.server_state.async_stubs[acceptors.get(i)].phaseone(phase_one_request, phase_one_observer);
                }

                checkDelay();
                phase_one_collector.waitUntilDone();
                if (phase_one_processor.getPromised() == false) {
                    ballot_aborted = true;
                    int maxballot = phase_one_processor.getHighballot();
                    if (maxballot > this.server_state.getCurrentBallot()) {
                        this.server_state.setCurrentBallot(maxballot);
                    }
                } else if (phase_one_processor.getLowballot() > -1) {
                    phase_two_value = phase_one_processor.getValue();
                }
                phase_one_readballot = phase_one_processor.getHighballot();
                System.out.println("Paxos phase 1 ended with aborted = " + ballot_aborted + " and read ballot = " + phase_one_readballot + " and value " + phase_two_value);

                // Paxos Phase Two
                if (ballot_aborted == false) {
                    System.out.println("Going to run paxos phase 2");

                    // send phase2
                    DidaMeetingsPaxos.PhaseTwoRequest.Builder phase_two_request = DidaMeetingsPaxos.PhaseTwoRequest.newBuilder();
                    phase_two_request.setInstance(entry_number);
                    phase_two_request.setRequestballot(ballot);
                    phase_two_request.setValue(phase_two_value);

                    PhaseTwoResponseProcessor phase_two_processor = new PhaseTwoResponseProcessor(quorum);

                    System.out.println("Calling peers with phase_two_request = " + phase_two_request);
                    ArrayList<DidaMeetingsPaxos.PhaseTwoReply> phase_two_responses = new ArrayList<DidaMeetingsPaxos.PhaseTwoReply>();
                    GenericResponseCollector<DidaMeetingsPaxos.PhaseTwoReply> phase_two_collector = new GenericResponseCollector<DidaMeetingsPaxos.PhaseTwoReply>(phase_two_responses, n_acceptors, phase_two_processor);
                    for (int i = 0; i < n_acceptors; i++) {
                        CollectorStreamObserver<DidaMeetingsPaxos.PhaseTwoReply> phase_two_observer = new CollectorStreamObserver<DidaMeetingsPaxos.PhaseTwoReply>(phase_two_collector);
                        this.server_state.async_stubs[acceptors.get(i)].phasetwo(phase_two_request.build(), phase_two_observer);
                    }

                    System.out.println("Waiting for responses...");
                    phase_two_collector.waitUntilDone();
                    if (phase_two_processor.getAccepted() == false) {
                        ballot_aborted = true;
                        this.server_state.setCurrentBallot(phase_two_processor.getMaxballot());
                    }
                    System.out.println("Paxos phase 2 ended with ballot_aborted = " + ballot_aborted);
                }

                // After phase2
                if (ballot_aborted == false) {
                    this.server_state.setCompletedBallot(ballot);
                    next_entry.command_id = phase_two_value;
                    next_entry.decided = true;
                }
            }
            if (next_entry.decided == false) {
                System.out.println("Entry not decided: waiting");
                this.has_work = false;
                while (this.has_work == false || (this.server_state.getDebugMode() == 1)) {
                    try {
                        checkCrash();
                        wait();
                    } catch (InterruptedException e) {
                    }
                }
            }

        }
        */

        System.out.println("Log entry with number " + next_entry.instance_nb + " has been decided with command id = " + next_entry.command_id);
        RequestRecord request_record = this.server_state.req_history.getIfInProcess(next_entry.command_id);
        // if I receive the paxos decision before the request
        while (request_record == null || (this.server_state.getDebugMode() == 1)) {
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
    }
}