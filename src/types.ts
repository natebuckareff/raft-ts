import type { RNG } from './rng';

export type PeerID = number & { readonly PeerID: unique symbol };
export type MessageID = number & { readonly MessageID: unique symbol };

export interface Raft {
    config: RaftConfig;
    state: RaftState;
}

export interface RaftConfig {
    id: PeerID;
    peers: PeerID[];
    rng: RNG;
    log: RaftLogger;
    electionInterval: [number, number] | undefined;
    heartbeatTimeout: number;
}

export interface RaftLogger {
    <Args extends any[]>(raft: Raft, time: number, message: string, ...args: Args): void;
}

export interface RaftState {
    persistent: RaftPersistent;
    volatile: RaftVolatile;
    sm: RaftSM;
}

export interface RaftPersistent {
    currentTerm: number;
    votedFor: PeerID | null;
    log: LogEntry[];
}

export interface LogEntry {
    term: number;
    command:
        | { type: 'SET'; name: string; value: number }
        | { type: 'INCR'; name: string; amount: number }
        | { type: 'DELETE'; name: string };
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
    pendingCalls: PendingAppendEntries[];
    nextIndex: Map<PeerID, number>;
    matchIndex: Map<PeerID, number>;
}

export interface PendingAppendEntries {
    message: AppendEntriesCall;
    timeSent: number;
    firstIndex: number;
}

export type StepResult = RaftMessage[];

export type RaftMessage =
    | ProposeCall
    | ProposeReply
    | AppendEntriesCall
    | AppendEntriesReply
    | RequestVoteCall
    | RequestVoteReply;

export interface ProposeCall {
    type: 'CALL';
    name: 'PROPOSE';
    id: MessageID;
    from: PeerID;
    to: PeerID;
    commands: LogEntry['command'][];
}

export interface ProposeReply {
    type: 'REPLY';
    name: 'PROPOSE';
    from: PeerID;
    to: PeerID;
    success: boolean;
}

export interface AppendEntriesCall {
    type: 'CALL';
    name: 'APPEND_ENTRIES';
    from: PeerID;
    to: PeerID;
    term: number;
    leaderId: PeerID; // TODO: Is this redundant with `from`?
    prevLogIndex: number;
    prevLogTerm: number;
    entries: LogEntry[];
    leaderCommit: number;
}

export interface AppendEntriesReply {
    type: 'REPLY';
    name: 'APPEND_ENTRIES';
    from: PeerID;
    to: PeerID;
    term: number;
    success: boolean;
    firstIndex?: number; // To identify the original CALL
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

export interface CreateArgs {
    config: RaftConfig;
    persistent?: RaftPersistent;
    time: number;
}
