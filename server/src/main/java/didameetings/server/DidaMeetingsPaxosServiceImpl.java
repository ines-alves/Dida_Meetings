package didameetings.server;

import java.util.*;

import didameetings.DidaMeetingsMain;
import didameetings.DidaMeetingsPaxos;
import didameetings.DidaMeetingsPaxos.LongPhaseOneReply;
import didameetings.DidaMeetingsPaxosServiceGrpc;

import didameetings.util.GenericResponseCollector;
import didameetings.util.CollectorStreamObserver;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.Context;

public class DidaMeetingsPaxosServiceImpl extends DidaMeetingsPaxosServiceGrpc.DidaMeetingsPaxosServiceImplBase {

    DidaMeetingsServerState server_state;

    public DidaMeetingsPaxosServiceImpl(DidaMeetingsServerState state) {
        this.server_state = state;
    }

    @Override
    public void longPhaseone(DidaMeetingsPaxos.PhaseOneRequest request, StreamObserver<DidaMeetingsPaxos.LongPhaseOneReply> responseObserver) {
        
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
        

        int instance = request.getInstance();
        int ballot = request.getRequestballot();
        System.out.println("LONG PHASE 1 REQUEST - INSTANCE: " + instance );
        PaxosInstance entry = this.server_state.paxos_log.testAndSetEntry(instance, ballot);
        boolean promised = false;
 
        if (ballot >= this.server_state.getCurrentBallot()) {
            promised = true;
            this.server_state.setCurrentBallot(ballot);
            entry.read_ballot = ballot;
        }

        int maxballot = this.server_state.getCurrentBallot();

        ArrayList<PaxosInstance> undecidedInstances = this.server_state.paxos_log.getUndecidedInstances();
        ArrayList<DidaMeetingsPaxos.PhaseOneReply> longPhaseOneReply = new ArrayList<DidaMeetingsPaxos.PhaseOneReply>();
        for (PaxosInstance undecidedInstance : undecidedInstances) {
            

            DidaMeetingsPaxos.PhaseOneReply.Builder undecided_response_builder = DidaMeetingsPaxos.PhaseOneReply.newBuilder();
            undecided_response_builder.setInstance(undecidedInstance.instance_nb);
            undecided_response_builder.setServerid(this.server_state.my_id);
            undecided_response_builder.setRequestballot(ballot);
            undecided_response_builder.setPromised(promised);
            undecided_response_builder.setValue(undecidedInstance.command_id);
            undecided_response_builder.setValballot(undecidedInstance.write_ballot);
            undecided_response_builder.setMaxballot(maxballot); 

            DidaMeetingsPaxos.PhaseOneReply undecided_response = undecided_response_builder.build();
            longPhaseOneReply.add(undecided_response);
        }

        DidaMeetingsPaxos.LongPhaseOneReply.Builder response_builder = DidaMeetingsPaxos.LongPhaseOneReply.newBuilder();
        response_builder.addAllLongPhaseOne(longPhaseOneReply);
        DidaMeetingsPaxos.LongPhaseOneReply response = response_builder.build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void phasetwo(DidaMeetingsPaxos.PhaseTwoRequest request, StreamObserver<DidaMeetingsPaxos.PhaseTwoReply> responseObserver) {
        int instance = request.getInstance();
        int ballot = request.getRequestballot();
        int value = request.getValue();
        System.out.println("PHASE 2 REQUEST - INSTANCE: " + instance );
        PaxosInstance entry = this.server_state.paxos_log.testAndSetEntry(instance);
        boolean accepted = false;
        int maxballot = ballot;

        if (ballot >= this.server_state.getCurrentBallot()) {
            accepted = true;
            entry.command_id = value;
            entry.write_ballot = ballot;
            entry.accept_ballot = ballot; // FIXME is this right?
            this.server_state.setCurrentBallot(ballot);
            this.server_state.req_history.moveToInProcess(value); //changed this to inside this if was in line 113
        
        } else {
            maxballot = this.server_state.getCurrentBallot();
        }
       
        DidaMeetingsPaxos.PhaseTwoReply.Builder response_builder = DidaMeetingsPaxos.PhaseTwoReply.newBuilder();
        response_builder.setAccepted(accepted);
        response_builder.setInstance(instance);
        response_builder.setServerid(this.server_state.my_id);
        response_builder.setRequestballot(ballot);
        response_builder.setMaxballot(maxballot);
        
        DidaMeetingsPaxos.PhaseTwoReply response = response_builder.build();
        
        responseObserver.onNext(response);
        responseObserver.onCompleted();
       
        // Notify learners
        if (accepted == true) {
            
            Context ctx = Context.current().fork();
            ctx.run(() -> {
                List<Integer> learners = this.server_state.scheduler.learners(ballot);
                int n_targets = learners.size();
                
                DidaMeetingsPaxos.LearnRequest.Builder learn_request_builder = DidaMeetingsPaxos.LearnRequest.newBuilder();
                learn_request_builder.setInstance(instance);
                learn_request_builder.setValue(value);
                learn_request_builder.setBallot(ballot);
                
                DidaMeetingsPaxos.LearnRequest learn_request = learn_request_builder.build();

                System.out.println("Paxos acceptor: going to notify learners for entry " + instance + " with timestamp " + ballot + " request = " + learn_request);
                ArrayList<DidaMeetingsPaxos.LearnReply> learn_responses = new ArrayList<DidaMeetingsPaxos.LearnReply>();
                GenericResponseCollector<DidaMeetingsPaxos.LearnReply> learn_collector = new GenericResponseCollector<DidaMeetingsPaxos.LearnReply>(learn_responses, n_targets);
                for (int i = 0; i < n_targets; i++) {
                    CollectorStreamObserver<DidaMeetingsPaxos.LearnReply> learn_observer = new CollectorStreamObserver<DidaMeetingsPaxos.LearnReply>(learn_collector);
                    this.server_state.async_stubs[learners.get(i)].learn(learn_request, learn_observer);
                }
                // System.out.println("Learn request completed for instance = " + instance);
            });
        }

    }

    @Override
    public void learn(DidaMeetingsPaxos.LearnRequest request, StreamObserver<DidaMeetingsPaxos.LearnReply> responseObserver) {
      
        int instance = request.getInstance();
        int ballot = request.getBallot();
        int value = request.getValue();

        synchronized (this) {
            System.out.println("Learn PHASE REQUEST - INSTANCE: " + instance );
            PaxosInstance entry = this.server_state.paxos_log.testAndSetEntry(instance);
            this.server_state.setCurrentBallot(ballot);

            if (ballot == entry.accept_ballot) {
                entry.n_accepts++;
                System.out.println("Paxos learner for instance " + instance + " : number of accepts " + entry.n_accepts);
                if (entry.n_accepts >= this.server_state.scheduler.quorum(ballot)) {
                    this.server_state.updateCompletedBallot(ballot);
                    entry.decided = true;
                    System.out.println("VALUE DECIDED = " + value);
                    this.server_state.req_history.getIfExists(value).setIsDecided(true);
                    this.server_state.main_loop.learn();
                }
            } else if (ballot > entry.accept_ballot) {
                System.out.println("Paxos learner for instance " + instance + " : resetting ");
                entry.command_id = value;
                entry.accept_ballot = ballot;
                entry.n_accepts = 1;
            }
        }

        DidaMeetingsPaxos.LearnReply.Builder response_builder = DidaMeetingsPaxos.LearnReply.newBuilder();
        response_builder.setInstance(instance);
        response_builder.setBallot(ballot);

        DidaMeetingsPaxos.LearnReply response = response_builder.build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

}
