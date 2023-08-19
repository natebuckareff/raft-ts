import assert from 'assert';
import { RaftServer } from './raft-server.js';
import { XoroRng } from './rng.js';
import { TaskScheduler, createRandomStrategy } from './test/task-scheduler.js';
import { DeterministicTime } from './time.js';
import type { PeerID, RaftMessage } from './types.js';
import { log } from './util.js';

type KVCommand = { type: 'SET'; name: string; value: number } | { type: 'DELETE'; name: string };

const rng = new XoroRng('blag');
const time = new DeterministicTime();

const sched = new TaskScheduler<RaftMessage<KVCommand>>({
    router: (_, { to }) => to,
    strategy: createRandomStrategy(rng),
});

const ids: PeerID[] = [0, 1, 2] as PeerID[];
const servers: RaftServer<KVCommand>[] = [];

for (const peerId of ids) {
    sched.spawn(function* (id, receive) {
        assert(id === peerId);

        const server = new RaftServer<KVCommand>(time, {
            id: peerId,
            peers: ids.filter(x => x !== id),
            rng,
            log: (raft, time, message, ...args) => log(raft, time, message, ...args),
            heartbeatTimeout: 60,
        });

        servers.push(server);

        while (true) {
            const message = receive();
            if (message !== undefined) {
                server.write(message);
            }

            for (const x of server.update()) {
                console.log('>>>', id, x);
            }

            yield;

            for (const message of server.read()) {
                yield message;
            }
        }
    });
}

for (let i = 0; i < 400; ++i) {
    sched.run();
    await new Promise(resolve => setTimeout(resolve, 2));
}

for (const server of servers) {
    console.log(server.raft.state.sm.status);
}
