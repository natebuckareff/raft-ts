import { create, step } from './raft.js';
import type { RNG } from './rng.js';
import type { Logger, PeerID, Raft, RaftConfig, RaftMessage } from './types.js';

async function intervalLoop(maxIterations: number, timestep: number, fn: (time: number) => void) {
    return new Promise<number>(resolve => {
        let iterations = 0;

        const intervalId = setInterval(() => {
            // Use fake time for determinism
            const time = (iterations + 1) * timestep;

            if (iterations > maxIterations) {
                clearInterval(intervalId);
                resolve(time);
                return;
            }

            fn(time);

            iterations += 1;
        }, timestep);
    });
}

interface Inbox<Cmd> {
    queue: RaftMessage<Cmd>[];
    receive: () => RaftMessage<Cmd> | undefined;
}

class InboxSet<Cmd> {
    private _inboxes: Map<PeerID, Inbox<Cmd>>;

    constructor() {
        this._inboxes = new Map();
    }

    get(id: PeerID): Inbox<Cmd> {
        let inbox = this._inboxes.get(id);
        if (inbox === undefined) {
            inbox = { queue: [], receive: () => this.receive(id) };
            this._inboxes.set(id, inbox);
        }
        return inbox;
    }

    send(message: RaftMessage<Cmd>): void {
        const { to } = message;
        this.get(to).queue.push(message);
    }

    receive(id: PeerID): RaftMessage<Cmd> | undefined {
        return this.get(id).queue.shift();
    }
}

export interface ExperimentConfig<Cmd> {
    peerCount: number;
    rng: RNG;
    log: Logger<Cmd>;
    electionInterval: [number, number] | undefined;
    heartbeatTimeout: number;
    maxIterations: number;
    timestep: number;
    hooks: Hook<Cmd>[];
    finished: Finished<Cmd>;
}

export interface Hook<Cmd> {
    (servers: Raft<Cmd>[], time: number): void;
}

export interface Finished<Cmd> {
    (servers: Raft<Cmd>[], time: number): void;
}

export async function runExperiment<Cmd>(config: ExperimentConfig<Cmd>) {
    const allPeers: PeerID[] = [];

    for (let i = 0; i < config.peerCount; ++i) {
        allPeers.push(i as PeerID);
    }

    const { rng, log, electionInterval, heartbeatTimeout } = config;

    const servers: Raft<Cmd>[] = [];

    for (const id of allPeers) {
        const peers = allPeers.filter(x => x !== id);
        const config: RaftConfig<Cmd> = { id, peers, rng, log, electionInterval, heartbeatTimeout };
        servers.push(create({ config, time: 0 }));
    }

    const inboxes = new InboxSet<Cmd>();

    const fn = (time: number) => {
        const messages: RaftMessage<Cmd>[] = [];

        for (const hook of config.hooks) {
            hook(servers, time);
        }

        for (const server of servers) {
            const { queue, receive } = inboxes.get(server.config.id);
            do {
                const result = step<Cmd>(server, time, receive);
                if (result !== undefined) {
                    if (result.type === 'SEND') {
                        if ('message' in result) {
                            messages.push(result.message);
                        } else {
                            messages.push(...result.messages);
                        }
                    }
                }
            } while (queue.length > 0);
        }

        for (const message of messages) {
            inboxes.send(message);
        }
    };

    const finalTime = await intervalLoop(config.maxIterations, config.timestep, fn);

    config.finished(servers, finalTime);
}
