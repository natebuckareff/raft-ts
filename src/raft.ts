import assert from 'assert';
import type {
    AppendEntriesCall,
    AppendEntriesReply,
    CreateArgs,
    LogEntry,
    ProposeCall,
    Raft,
    RaftConfig,
    RaftMessage,
    RequestVoteCall,
    RequestVoteReply,
    StepResult,
} from './types';
import { randomInterval } from './util.js';

export function create({ config, persistent, time }: CreateArgs): Raft {
    return {
        config,
        state: {
            persistent: persistent ?? {
                // paper(Figure 2): initialized to 0 on first boot, increases
                // monotonically
                currentTerm: 0,

                // paper(Figure 2): candidateId that received vote in current
                // term (or null if none)
                votedFor: null,

                log: [],
            },
            volatile: {
                // paper(Figure 2): initialized to 0, increases monotonically
                commitIndex: 0,

                // paper(Figure 2): initialized to 0, increases monotonically
                lastApplied: 0,
            },
            sm: {
                // paper(5.2):> When servers start up, they begin as followers.
                status: 'FOLLOWER',

                // paper(5.2): 150–300ms
                electionTimeout: getElectionTimeout(config, time),
            },
        },
    };
}

export function step(raft: Raft, time: number, receive: () => RaftMessage | undefined): StepResult {
    // TODO:
    // - [ ] Figure out when to do the `commitIndex > lastApplied` check and how

    const { sm } = raft.state;

    if (sm.status === 'FOLLOWER' || sm.status === 'CANDIDATE') {
        if (time >= sm.electionTimeout) {
            // paper(Figure 2): If election timeout elapses without receiving
            // AppendEntries RPC from current leader or granting vote to
            // candidate: convert to candidate

            // paper(Figure 2): If election timeout elapses: start new election

            // paper(5.2): if many followers become candidates at the same time,
            // votes could be split so that no candidate obtains a majority.
            // When this happens, each candidate will time out and start a new
            // election by incrementing its term and initiating another round of
            // RequestVote RPCs.

            // NOTE: In the case of both follower and candidate, the effect of
            // `becomeCandidate` is the same: to become a candidate if not
            // already one and start a new election

            return becomeCandidate(raft, time);
        }
    }

    if (sm.status === 'LEADER') {
        let results: StepResult | undefined;

        if (time >= sm.heartbeatTimeout) {
            // paper(Figure 2): Upon election: send initial empty AppendEntries
            // RPCs (heartbeat) to each server; repeat during idle periods to
            // prevent election timeouts (§5.2)

            (results ??= []).push(...sendHeartbeats(raft, time));
        }

        for (const pendingCall of sm.pendingCalls) {
            if (time >= pendingCall.timeSent) {
                // paper(5.3): If followers crash or run slowly, or if network
                // packets are lost, the leader retries AppendEntries RPCs
                // indefinitely (even after it has responded to the client)
                // until all followers eventually store all log entries.

                pendingCall.timeSent = time;

                (results ??= []).push(pendingCall.message);
            }
        }

        if (results !== undefined) {
            return results;
        }
    }

    const message = receive();

    if (message !== undefined) {
        if (message.name === 'PROPOSE') {
            // Handle client requests

            if (message.type === 'CALL') {
                return handleProposeCall(raft, time, message);
            } else {
                // TODO: Drop the invalid message
                return [];
            }
        }

        if (message.term > raft.state.persistent.currentTerm) {
            // paper(Figure 2): If RPC request or response contains term T >
            // currentTerm: set currentTerm = T, convert to follower (§5.1)

            becomeFollower(raft, time, message.term);
        }

        if (message.term < raft.state.persistent.currentTerm) {
            // TODO: Does this rule apply only to AppendEntries and RequestVote?
            // What about other RPCs?

            // paper(Figure 2): Reply false if term < currentTerm (§5.1)

            if (message.type === 'CALL') {
                if (message.name === 'APPEND_ENTRIES') {
                    return [
                        {
                            type: 'REPLY',
                            name: 'APPEND_ENTRIES',
                            from: raft.config.id,
                            to: message.from,
                            term: raft.state.persistent.currentTerm,
                            success: false,
                        },
                    ];
                }

                if (message.name === 'REQUEST_VOTE') {
                    return [
                        {
                            type: 'REPLY',
                            name: 'REQUEST_VOTE',
                            from: raft.config.id,
                            to: message.from,
                            term: raft.state.persistent.currentTerm,
                            voteGranted: false,
                        },
                    ];
                }
            }

            // TODO: Is dropping the message the correct thing to do here?
            return [];
        }

        switch (message.name) {
            case 'APPEND_ENTRIES':
                if (message.type === 'CALL') {
                    raft.config.log(raft, time, 'received AppendEntries', message.from);
                    return handleAppendEntriesCall(raft, time, message);
                } else {
                    return handleAppendEntriesReply(raft, message);
                }

            case 'REQUEST_VOTE':
                if (message.type === 'CALL') {
                    return handleRequestVotesCall(raft, time, message);
                } else {
                    return handleRequestVotesReply(raft, time, message);
                }
        }
    }

    // paper(Figure 2): If commitIndex > lastApplied: increment lastApplied,
    // apply log[lastApplied] to state machine (§5.3)

    // XXX
    return [];
}

function getElectionTimeout(config: RaftConfig, time: number): number {
    // XXX TODO: Slowed down

    // paper(5.2): 150–300ms
    const interval = config.electionInterval ?? [150, 300];

    return time + randomInterval(config.rng, ...interval);
}

function getHeartbeatTimeout(raft: Raft, time: number): number {
    return time + raft.config.heartbeatTimeout;
}

function becomeCandidate(raft: Raft, time: number): StepResult {
    // TODO:
    // - [x] Implement Figure 2
    // - [ ] Figure out how what `lastLogIndex` and `lastLogTerm` should be

    // paper(Figure 2): On conversion to candidate, start election:
    // • Increment currentTerm
    // • Vote for self
    // • Reset election timer
    // • Send RequestVote RPCs to all other servers

    raft.config.log(raft, time, 'started candidacy');

    const electionTimeout = getElectionTimeout(raft.config, time);

    raft.state.persistent.currentTerm += 1;
    raft.state.persistent.votedFor = raft.config.id;
    raft.state.sm = {
        status: 'CANDIDATE',
        electionTimeout,
        votesReceived: new Set([raft.config.id]),
    };

    const result: StepResult = [];
    for (const to of raft.config.peers) {
        raft.config.log(raft, time, `sending REQUEST_VOTE to ${to}`);

        result.push({
            type: 'CALL',
            name: 'REQUEST_VOTE',
            from: raft.config.id,
            to,
            term: raft.state.persistent.currentTerm,
            candidateId: raft.config.id,
            lastLogIndex: 0, // TODO
            lastLogTerm: 0, // TODO
        });
    }

    return result;
}

function becomeFollower(raft: Raft, time: number, discoveredTerm: number): void {
    // Becoming a follower because another term was discovered, so switch to
    // that term
    raft.state.persistent.currentTerm = discoveredTerm;

    // Unset this since we're going to be voting in a new term
    raft.state.persistent.votedFor = null;

    raft.state.sm = {
        status: 'FOLLOWER',
        electionTimeout: getElectionTimeout(raft.config, time),
    };
}

function resetElectionTimeout(raft: Raft, time: number): void {
    assert(raft.state.sm.status !== 'LEADER');

    raft.state.sm.electionTimeout = getElectionTimeout(raft.config, time);
}

function becomeLeader(raft: Raft, time: number): StepResult {
    // sends heartbeats at some point

    raft.state.sm = {
        status: 'LEADER',
        heartbeatTimeout: 0, // Set in `sendHeartbeats`
        pendingCalls: [],
        nextIndex: new Map(), // XXX
        matchIndex: new Map(), // XXX
    };

    raft.config.log(raft, time, 'became leader');

    return sendHeartbeats(raft, time);
}

function sendHeartbeats(raft: Raft, time: number): StepResult {
    assert(raft.state.sm.status === 'LEADER');

    raft.state.sm.heartbeatTimeout = getHeartbeatTimeout(raft, time);

    raft.config.log(raft, time, 'sending heartbeats');

    const result: StepResult = [];

    for (const to of raft.config.peers) {
        result.push({
            type: 'CALL',
            name: 'APPEND_ENTRIES',
            from: raft.config.id,
            to,
            term: raft.state.persistent.currentTerm,
            leaderId: raft.config.id,
            prevLogIndex: 0, // XXX
            prevLogTerm: 0, // XXX
            entries: [],
            leaderCommit: 0, // XXX
        });
    }

    return result;
}

function handleProposeCall(raft: Raft, time: number, message: ProposeCall): StepResult {
    const { sm } = raft.state;

    if (sm.status !== 'LEADER') {
        return [
            {
                type: 'REPLY',
                name: 'PROPOSE',
                from: raft.config.id,
                to: message.from,
                success: false,
            },
        ];
    }

    // paper(5.3): Once a leader has been elected, it begins servicing client
    // requests. Each client request contains a command to be executed by the
    // replicated state machines. The leader appends the command to its log as a
    // new entry, then issues AppendEntries RPCs in parallel to each of the
    // other servers to replicate the entry.

    const entries: LogEntry[] = [];

    for (const command of message.commands) {
        entries.push({
            term: raft.state.persistent.currentTerm,
            command,
        });
    }

    const firstIndex = raft.state.persistent.log.length + 1;

    raft.state.persistent.log.push(...entries);

    const result: StepResult = [];

    for (const to of raft.config.peers) {
        const message: AppendEntriesCall = {
            type: 'CALL',
            name: 'APPEND_ENTRIES',
            from: raft.config.id,
            to,
            term: raft.state.persistent.currentTerm,
            leaderId: raft.config.id,
            prevLogIndex: 0, // XXX
            prevLogTerm: 0, // XXX
            entries, // TODO,
            leaderCommit: 0, // XXX
        };

        sm.pendingCalls.push({
            message,
            timeSent: time,
            firstIndex,
        });

        result.push(message);
    }

    // TODO: Need a way to track the progress of these entries so that the
    // client can be notified when it is replicated

    return result;
}

function handleAppendEntriesCall(raft: Raft, time: number, message: AppendEntriesCall): StepResult {
    assert(raft.state.persistent.currentTerm === message.term);

    // TODO:
    // - [ ] Actually update the log

    // Receiver implementation:
    // 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
    //    matches prevLogTerm (§5.3)
    // 3. If an existing entry conflicts with a new one (same index but
    //    different terms), delete the existing entry and all that follow it
    //    (§5.3)
    // 4. Append any new entries not already in the log
    // 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit,
    //    index of last new entry)

    const { sm } = raft.state;

    if (sm.status === 'FOLLOWER') {
        resetElectionTimeout(raft, time);
    }

    if (sm.status !== 'FOLLOWER') {
        // Justification for candidate: paper(5.2): While waiting for votes, a
        // candidate may receive an AppendEntries RPC from another server
        // claiming to be leader. If the leader’s term (included in its RPC) is
        // at least as large as the candidate’s current term, then the candidate
        // recognizes the leader as legitimate and returns to follower state.

        // TODO: Justification for leader

        // TODO: Justification for returning `success = true` when we didn't
        // even consider entries

        // Return to the follower state with term equal to the leader sending
        // AppendEntries. `currentTerm` will be set by `becomeFollower`
        const discoveredTerm = message.term;

        becomeFollower(raft, time, discoveredTerm);

        return [
            {
                type: 'REPLY',
                name: 'APPEND_ENTRIES',
                from: raft.config.id,
                to: message.from,
                term: discoveredTerm,
                success: true,
            },
        ];
    }

    return [];
}

function handleAppendEntriesReply(raft: Raft, message: AppendEntriesReply): StepResult {
    assert(raft.state.persistent.currentTerm === message.term);

    const { sm } = raft.state;

    if (sm.status === 'LEADER') {
        if (message.success === true && message.firstIndex !== undefined) {
            // TODO: Does `success` true mean that replication happened? Because
            // the paper makes it seem like it actualy just has to do with a
            // validation check. But maybe that check only happens if
            // replication also happens...

            sm.pendingCalls = sm.pendingCalls.filter(x => {
                // Cleanup any pending AppendEntry call(s) that this reply is in
                // response to

                if (x.message.to === message.from && x.firstIndex === message.firstIndex) {
                    return false;
                }
                return true;
            });
        }
    }

    // TODO: Nothing really to do here? Do we do anything when `message.success`
    // is false?

    return [];
}

function handleRequestVotesCall(raft: Raft, time: number, message: RequestVoteCall): StepResult {
    assert(raft.state.persistent.currentTerm === message.term);

    // TODO:
    // - [ ] Check that log is at least as up-to-date

    // Receiver implementation:
    // 2. If votedFor is null or candidateId, and candidate’s log is at least as
    //    up-to-date as receiver’s log, grant vote (§5.2, §5.4)

    const { sm } = raft.state;

    if (sm.status === 'FOLLOWER' || sm.status === 'CANDIDATE') {
        // paper(5.2): Each server will vote for at most one candidate in a
        // given term, on a first-come-first-served basis

        const { votedFor } = raft.state.persistent;

        if (votedFor === null || votedFor === message.candidateId) {
            raft.state.persistent.votedFor = message.candidateId;
            sm.electionTimeout = getElectionTimeout(raft.config, time);
            return [
                {
                    type: 'REPLY',
                    name: 'REQUEST_VOTE',
                    from: raft.config.id,
                    to: message.from,
                    term: raft.state.persistent.currentTerm,
                    voteGranted: true,
                },
            ];
        }
    }

    return [
        {
            type: 'REPLY',
            name: 'REQUEST_VOTE',
            from: raft.config.id,
            to: message.from,
            term: raft.state.persistent.currentTerm,
            voteGranted: false,
        },
    ];
}

function handleRequestVotesReply(raft: Raft, time: number, message: RequestVoteReply): StepResult {
    assert(raft.state.persistent.currentTerm === message.term);

    const sm = raft.state.sm;

    if (sm.status !== 'CANDIDATE') {
        return [];
    }

    if (message.voteGranted) {
        // paper(5.2): Each server will vote for at most one candidate

        // Make sure that each vote is only counted once per server. This is in
        // case of message duplication
        sm.votesReceived.add(message.from);

        raft.config.log(raft, time, 'vote granted by', message.from);

        if (sm.votesReceived.size * 2 > raft.config.peers.length + 1) {
            // paper(5.2): A candidate wins an election if it receives votes
            // from a majority of the servers in the full cluster for the same
            // term.

            raft.config.log(raft, time, `won election with ${sm.votesReceived.size} votes`);

            return becomeLeader(raft, time);
        }
    } else {
        raft.config.log(raft, time, 'vote denied by', message.from);
    }

    return [];
}
