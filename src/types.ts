import type { RNG } from './rng';

export type PeerID = number & { readonly PeerID: unique symbol };
export type MessageID = number & { readonly MessageID: unique symbol };

export interface Raft<Cmd> {
    config: RaftConfig<Cmd>;
    state: RaftState<Cmd>;
}

export interface RaftConfig<Cmd> {
    id: PeerID;
    peers: PeerID[];
    rng: RNG;
    log: Logger<Cmd>;
    electionInterval: [number, number] | undefined;
    heartbeatTimeout: number;
}

export interface Logger<Cmd> {
    <Args extends any[]>(raft: Raft<Cmd>, time: number, message: string, ...args: Args): void;
}

export interface RaftState<Cmd> {
    persistent: RaftPersistent<Cmd>;
    volatile: RaftVolatile;
    sm: RaftSM;
}

export interface RaftPersistent<Cmd> {
    currentTerm: number;
    votedFor: PeerID | null;
    log: LogEntry<Cmd>[];
}

export interface LogEntry<Cmd> {
    index: number;
    term: number;
    command: Cmd;
}

export interface RaftVolatile {
    commitIndex: number;
    lastApplied: number;
}

export type RaftSM = FollowerState | CandidateState | LeaderState;

export interface FollowerState {
    status: 'FOLLOWER';
    electionTimeout: number;
}

export interface CandidateState {
    status: 'CANDIDATE';
    electionTimeout: number;
    votesReceived: Set<PeerID>;
}

export interface LeaderState {
    status: 'LEADER';
    heartbeatTimeout: number;
    nextIndex: Map<PeerID, number>;
    matchIndex: Map<PeerID, number>;
    proposalCommitQueue: ProposalEntry[];
}

// When the leader receives a ProposeCommandCall from a client, it appends the
// new entry to the log, and pushes a ProposalEntry onto the proposal commit
// queue. When the first entry added by the proposal is committed, the leader
// will a ProposeCommandReply back to the client
export interface ProposalEntry {
    startIndex: number;
    entryCount: number;
    clientId: PeerID;
}

// TODO: This could also be used for logging and any other side effects that
// don't require returning anything

export type StepResult<Cmd> =
    | { type: 'SEND'; message: RaftMessage<Cmd> }
    | { type: 'SEND'; messages: RaftMessage<Cmd>[] }
    | { type: 'APPLY'; entries: LogEntry<Cmd>[] }
    | void;

export type RaftMessage<Cmd> =
    | ProposeCommandCall<Cmd>
    | ProposeCommandReply
    | AppendEntriesCall<Cmd>
    | AppendEntriesReply
    | RequestVoteCall
    | RequestVoteReply;

export interface ProposeCommandCall<Cmd> {
    type: 'CALL';
    name: 'PROPOSE_COMMAND';
    id: MessageID;
    from: PeerID;
    to: PeerID;
    commands: Cmd[];
}

export interface ProposeCommandReply {
    type: 'REPLY';
    name: 'PROPOSE_COMMAND';
    from: PeerID;
    to: PeerID;
    success: boolean;
}

export interface AppendEntriesCall<Cmd> {
    type: 'CALL';
    name: 'APPEND_ENTRIES';
    from: PeerID;
    to: PeerID;
    term: number;
    leaderId: PeerID; // TODO: Is this redundant with `from`?
    prevLogIndex: number;
    prevLogTerm: number;
    entries: LogEntry<Cmd>[];
    leaderCommit: number;
}

export interface AppendEntriesReply {
    type: 'REPLY';
    name: 'APPEND_ENTRIES';
    from: PeerID;
    to: PeerID;
    term: number;
    success: boolean;

    // Included to update `nextIndex` without needing to complicate the leader's
    // internal state
    entryCount: number;
}

export interface RequestVoteCall {
    type: 'CALL';
    name: 'REQUEST_VOTE';
    from: PeerID;
    to: PeerID;
    term: number;
    candidateId: PeerID;
    lastLogIndex: number;
    lastLogTerm: number;
}

export interface RequestVoteReply {
    type: 'REPLY';
    name: 'REQUEST_VOTE';
    from: PeerID;
    to: PeerID;
    term: number;
    voteGranted: boolean;
}

export interface CreateArgs<Cmd> {
    config: RaftConfig<Cmd>;
    persistent?: RaftPersistent<Cmd>;
    time: number;
}
