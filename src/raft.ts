import assert from 'assert';
import type {
    AppendEntriesCall,
    AppendEntriesReply,
    CreateArgs,
    LeaderState,
    PeerID,
    ProposeCommandCall,
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
        if (time >= sm.heartbeatTimeout) {
            // paper(Figure 2): Upon election: send initial empty AppendEntries
            // RPCs (heartbeat) to each server; repeat during idle periods to
            // prevent election timeouts (§5.2)

            return sendHeartbeats(raft, time);
        }
    }

    const message = receive();

    if (message !== undefined) {
        if (message.name === 'PROPOSE_COMMAND') {
            // Handle client requests

            if (message.type === 'CALL') {
                return handleProposeCall(raft, time, message);
            } else {
                // TODO: Drop the invalid message
                return;
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
                    return {
                        type: 'SEND',
                        message: {
                            type: 'REPLY',
                            name: 'APPEND_ENTRIES',
                            from: raft.config.id,
                            to: message.from,
                            term: raft.state.persistent.currentTerm,
                            success: false,
                            entryCount: 0,
                        },
                    };
                }

                if (message.name === 'REQUEST_VOTE') {
                    return {
                        type: 'SEND',
                        message: {
                            type: 'REPLY',
                            name: 'REQUEST_VOTE',
                            from: raft.config.id,
                            to: message.from,
                            term: raft.state.persistent.currentTerm,
                            voteGranted: false,
                        },
                    };
                }
            }

            // TODO: Is dropping the message the correct thing to do here?
            return;
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

    return;
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

function getLastLogIndexAndTerm(raft: Raft): { lastLogIndex: number; lastLogTerm: number } {
    const { log } = raft.state.persistent;

    if (log.length === 0) {
        return { lastLogIndex: 0, lastLogTerm: 0 };
    }

    const { index: lastLogIndex, term: lastLogTerm } = log[log.length - 1]!;

    return { lastLogIndex, lastLogTerm };
}

function becomeCandidate(raft: Raft, time: number): StepResult {
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

    const { lastLogIndex, lastLogTerm } = getLastLogIndexAndTerm(raft);

    const messages: RaftMessage[] = [];

    for (const to of raft.config.peers) {
        raft.config.log(raft, time, `sending REQUEST_VOTE to ${to}`);

        messages.push({
            type: 'CALL',
            name: 'REQUEST_VOTE',
            from: raft.config.id,
            to,
            term: raft.state.persistent.currentTerm,
            candidateId: raft.config.id,
            lastLogIndex,
            lastLogTerm,
        });
    }

    return {
        type: 'SEND',
        messages,
    };
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

    // paper(Figure 2): for each server, index of the next log entry to send to
    // that server (initialized to leader log index + 1)
    const nextIndex = new Map<PeerID, number>();

    const { log } = raft.state.persistent;
    const leaderNextLogIndex = (log[log.length - 1]?.index ?? 0) + 1;

    for (const id of raft.config.peers) {
        nextIndex.set(id, leaderNextLogIndex);
    }

    // paper(Figure 2): for each server, index of highest log entry to be
    // replicated on server (initialized to 0, increases monotonically)
    const matchIndex = new Map<PeerID, number>();

    for (const id of raft.config.peers) {
        matchIndex.set(id, 0);
    }

    raft.state.sm = {
        status: 'LEADER',
        heartbeatTimeout: 0, // Set in `sendHeartbeats`
        nextIndex,
        matchIndex,
        proposalCommitQueue: [],
    };

    raft.config.log(raft, time, 'became leader');

    return sendHeartbeats(raft, time);
}

function sendHeartbeats(raft: Raft, time: number): StepResult {
    const { sm } = raft.state;

    assert(sm.status === 'LEADER');

    raft.config.log(raft, time, 'sending heartbeats');

    // Send (potentially empty) AppendEntries to all peers
    return replicateLog(raft, sm, time);
}

function handleProposeCall(raft: Raft, time: number, message: ProposeCommandCall): StepResult {
    // TODO: Message validation

    const { sm } = raft.state;

    if (sm.status !== 'LEADER') {
        // TODO: Help the client discover the leader PeerID
        return {
            type: 'SEND',
            message: {
                type: 'REPLY',
                name: 'PROPOSE_COMMAND',
                from: raft.config.id,
                to: message.from,
                success: false,
            },
        };
    }

    // paper(5.3): Once a leader has been elected, it begins servicing client
    // requests. Each client request contains a command to be executed by the
    // replicated state machines. The leader appends the command to its log as a
    // new entry, then issues AppendEntries RPCs in parallel to each of the
    // other servers to replicate the entry.

    // Push new commands onto the log

    const { log } = raft.state.persistent;

    // The first index for the new entries is the last index in the log or 1
    const startIndex = (log[log.length - 1]?.index ?? 0) + 1;

    for (let i = 0; i < message.commands.length; ++i) {
        const command = message.commands[i]!;
        const index = startIndex + i;

        raft.config.log(raft, time, `append entry ${index}`);

        log.push({
            index,
            term: raft.state.persistent.currentTerm,
            command,
        });
    }

    // TODO: What happens when a leader crashes? There will be some clients that
    // never receive a response. Need a way to make ProposeCommandCall
    // idempotent so that the client can call it multiple times to eventually
    // get back a response

    // Keep track of which entries were pushed by which clients. When an entry
    // is commited, send a ProposeCommandReply back to the original client
    sm.proposalCommitQueue.push({
        startIndex,
        entryCount: message.commands.length,
        clientId: message.from,
    });

    // Send AppendEntries to all peers
    return replicateLog(raft, sm, time);
}

function replicateLog(raft: Raft, sm: LeaderState, time: number): StepResult {
    // Reset the heartbeat timeout
    sm.heartbeatTimeout = getHeartbeatTimeout(raft, time);

    const messages: RaftMessage[] = [];

    for (const id of raft.config.peers) {
        const nextIndex = sm.nextIndex.get(id);

        // This should always be initialized to the last log entry index + 1 for
        // each peer. If the leader log is empty, it's initialized to 1 which
        // makes sense since that's the index of "next entry" that will be
        // appened to the log for an empty log
        assert(nextIndex !== undefined);

        // Index of the log entry that comes immediately before the first new
        // entry that the leader sends to the client. Zero when the log is empty
        const prevLogIndex = nextIndex - 1;

        const { log } = raft.state.persistent;
        const prevLogEntry = log[prevLogIndex - 1];

        // Term of the `prevLogIndex` entry. Zero when the log is empty. Notice
        // that no leader, and therefore no log entry, will ever actually have a
        // term equal to 0
        const prevLogTerm = prevLogEntry?.term ?? 0;

        // Get all new entries in the log starting at the array position
        // corresponding to `nextIndex`
        const entries = log.slice(nextIndex - 1);

        messages.push({
            type: 'CALL',
            name: 'APPEND_ENTRIES',
            from: raft.config.id,
            to: id,
            term: raft.state.persistent.currentTerm,
            leaderId: raft.config.id,
            prevLogIndex,
            prevLogTerm,
            entries,
            leaderCommit: raft.state.volatile.commitIndex,
        });
    }

    return {
        type: 'SEND',
        messages,
    };
}

function handleAppendEntriesCall(raft: Raft, time: number, message: AppendEntriesCall): StepResult {
    // TODO: Message validation

    assert(raft.state.persistent.currentTerm === message.term);

    const { sm } = raft.state;

    // paper(Figure 3): Election Safety: at most one leader can be elected in a
    // given term. §5.2

    assert(sm.status !== 'LEADER', 'not possible for two leaders to exist during the same term');

    if (sm.status === 'CANDIDATE') {
        // paper(5.2): If the leader’s term (included in its RPC) is at least as
        // large as the candidate’s current term, then the candidate recognizes
        // the leader as legitimate and returns to follower state.

        // Where "at least as large" means greater-than-or-equal

        becomeFollower(raft, time, message.term);
    } else {
        // If server was already a follower, reset the election timeout since

        // paper(5.2): When servers start up, they begin as followers. A server
        // remains in follower state as long as it receives valid RPCs from a
        // leader or candidate.

        resetElectionTimeout(raft, time);
    }

    // Sanity check. At this point the server is either already a follower or
    // was a candidate and reverted to being a follower
    assert(sm.status === 'FOLLOWER');

    // paper(5.3): When sending an AppendEntries RPC, the leader includes the
    // index and term of the entry in its log that immediately precedes the new
    // entries. If the follower does not find an entry in its log with the same
    // index and term, then it refuses the new entries.

    const { prevLogIndex, prevLogTerm } = message;
    const { log } = raft.state.persistent;

    // If the leader's log is empty OR the follower's log contains an entry at
    // `prevLogIndex` with term equal to `prevLogTerm`

    // prettier-ignore
    const isConsistent =
        prevLogIndex === 0 || (
            prevLogIndex <= log.length &&
            log[prevLogIndex - 1]!.term === prevLogTerm
        );

    if (isConsistent) {
        // The log position that the new entries will start at
        const startPos = prevLogIndex;

        // Entries already in the follower's log that will be deleted
        const deleteCount = log.length - startPos;

        // Splice in new entries
        log.splice(startPos, deleteCount, ...message.entries);

        // Update the peer's max committed entry index. It must not be greater
        // than the last entry replicated to this peer
        if (message.leaderCommit > raft.state.persistent.currentTerm) {
            raft.state.persistent.currentTerm = Math.min(message.leaderCommit, log.length);
        }
    }

    return {
        type: 'SEND',
        message: {
            type: 'REPLY',
            name: 'APPEND_ENTRIES',
            from: raft.config.id,
            to: message.from,
            term: raft.state.persistent.currentTerm,
            success: isConsistent,
            entryCount: message.entries.length,
        },
    };
}

function handleAppendEntriesReply(raft: Raft, message: AppendEntriesReply): StepResult {
    // TODO: Message validation

    assert(raft.state.persistent.currentTerm === message.term);

    const { sm } = raft.state;

    if (sm.status !== 'LEADER') {
        // TODO: Just drop the message?
        return;
    }

    const nextIndex = sm.nextIndex.get(message.from);
    const matchIndex = sm.matchIndex.get(message.from);

    // TODO: Should instead perform message validation before processing the
    // message. If we receive a message from a peer and we know about that
    // peer then other peer-related state should be valid
    assert(nextIndex !== undefined && matchIndex !== undefined);

    if (message.success === true) {
        // Advance `nextIndex` by the number of entries sent in the original
        // request
        sm.nextIndex.set(message.from, nextIndex + message.entryCount);

        // Peer has replicated entries starting at `nextIndex` up to the index
        // of the last entry sent
        sm.matchIndex.set(message.from, nextIndex + message.entryCount - 1);

        // If `entryCount == 0` then `nextIndex` doesn't change, but
        // `matchIndex` is updated to reflect that the peer has an entry
        // matching `prevLogIndex` and `prevLogTerm` in their log

        const { log } = raft.state.persistent;
        const { commitIndex } = raft.state.volatile;

        // Iterate through all uncommitted entries to find the largest entry
        // index that is replicated to a majority of followers
        for (let i = commitIndex + 1; i <= log.length; ++i) {
            // Count the leader once
            let replicationCount = 1;

            for (const id of raft.config.peers) {
                const matchIndex = sm.matchIndex.get(id);

                // TODO: Should make this a helper function...
                assert(matchIndex !== undefined);

                // Is the highest replicated entry index on this peer >= the
                // i'th uncommitted entry on the leader? If it is, then the i'th
                // entry has been replicated to that peer
                if (matchIndex >= i) {
                    replicationCount += 1;
                }
            }

            // Has i'th entry has been replicated to a majority of servers?
            if (replicationCount * 2 > raft.config.peers.length + 1) {
                raft.state.volatile.commitIndex = i;
            }
        }

        {
            // paper(Figure 2): If commitIndex > lastApplied: increment
            // lastApplied, apply log[lastApplied] to state machine (§5.3)

            const { commitIndex, lastApplied } = raft.state.volatile;

            if (commitIndex > lastApplied) {
                const entries = log.slice(lastApplied);
                raft.state.volatile.lastApplied = commitIndex;
                return { type: 'APPLY', entries };
            }
        }

        return;
    } else {
        sm.nextIndex.set(message.from, nextIndex - 1);

        // TODO: Immediately retry sending AppendEntries. Track peers that
        // replied with `sucess == false` with an array of PeerIDs
        // `pendingRetries: PeerID[]` which is cleared after heartbeats are sent

        // TODO: Send retries
        return;
    }
}

function handleRequestVotesCall(raft: Raft, time: number, message: RequestVoteCall): StepResult {
    // TODO: Validate message before processing

    assert(raft.state.persistent.currentTerm === message.term);

    // Receiver implementation:
    // 2. If votedFor is null or candidateId, and candidate’s log is at least as
    //    up-to-date as receiver’s log, grant vote (§5.2, §5.4)

    const { sm } = raft.state;

    if (sm.status === 'FOLLOWER' || sm.status === 'CANDIDATE') {
        // If the peer hasn't already voted for any candidates or they have
        // already voted for this candidate and are responding to a duplicate
        // RequestVotes RPC

        // paper(5.2): Each server will vote for at most one candidate in a
        // given term, on a first-come-first-served basis

        const { votedFor } = raft.state.persistent;

        if (votedFor === null || votedFor === message.candidateId) {
            const { lastLogIndex, lastLogTerm } = getLastLogIndexAndTerm(raft);

            // paper(5.4.1): [...] the voter denies its vote if its own log is
            // more up-to-date than that of the candidate.

            const logsSynced =
                message.lastLogTerm > lastLogTerm ||
                (message.lastLogTerm === lastLogTerm && message.lastLogIndex >= lastLogIndex);

            if (logsSynced) {
                raft.state.persistent.votedFor = message.candidateId;
                sm.electionTimeout = getElectionTimeout(raft.config, time);
                return {
                    type: 'SEND',
                    message: {
                        type: 'REPLY',
                        name: 'REQUEST_VOTE',
                        from: raft.config.id,
                        to: message.from,
                        term: raft.state.persistent.currentTerm,
                        voteGranted: true,
                    },
                };
            }
        }
    }

    return {
        type: 'SEND',
        message: {
            type: 'REPLY',
            name: 'REQUEST_VOTE',
            from: raft.config.id,
            to: message.from,
            term: raft.state.persistent.currentTerm,
            voteGranted: false,
        },
    };
}

function handleRequestVotesReply(raft: Raft, time: number, message: RequestVoteReply): StepResult {
    // TODO: Validate message before processing

    assert(raft.state.persistent.currentTerm === message.term);

    const sm = raft.state.sm;

    if (sm.status !== 'CANDIDATE') {
        return;
    }

    if (message.voteGranted) {
        // Make sure that each vote is only counted once per peer

        // paper(5.2): Each server will vote for at most one candidate

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

    return;
}
