import type { RNG } from './rng';
import type { CandidateState, FollowerState, LeaderState, Raft, RaftSM } from './types';

export const randomInterval = (rng: RNG, start: number, end: number) => {
    if (start > end) {
        throw Error('invalid interval');
    }
    return start + rng.random() * (end - start);
};

export function log<Cmd, Args extends any[]>(raft: Raft<Cmd>, time: number, message: string, ...args: Args) {
    const { id } = raft.config;
    const { status } = raft.state.sm;
    const { currentTerm } = raft.state.persistent;
    console.log(`[time=${time} id=${id} term=${currentTerm} status=${status}] ${message}`, ...args);
}

export function isFollower(sm: RaftSM): sm is FollowerState {
    return sm.status === 'FOLLOWER';
}

export function isCandidate(sm: RaftSM): sm is CandidateState {
    return sm.status === 'CANDIDATE';
}

export function isLeader(sm: RaftSM): sm is LeaderState {
    return sm.status === 'LEADER';
}
