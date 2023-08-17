import assert from 'assert';
import { runExperiment } from './experiment.js';
import { create } from './raft.js';
import { XoroRng } from './rng.js';
import { log } from './util.js';

async function sanityCheck(timeScale = 1) {
    console.log('--- sanityCheck ' + '-'.repeat(32 - 'sanityCheck'.length));
    await runExperiment({
        rng: new XoroRng('test2-2023-08-17'),
        log: (raft, time, message, ...args) => {
            return log(raft, time, message, ...args);
        },
        electionInterval: [150 * timeScale, 300 * timeScale],
        heartbeatTimeout: 60 * timeScale,
        peerCount: 3,
        timestep: 1,
        maxIterations: 500 * timeScale,
        hooks: [],
        finished: (servers, time) => {
            console.log('--- results ' + '-'.repeat(32 - 'results'.length));
            assert(time === 502 * timeScale);
            console.log(`time: ${time}`);
            const expect = [
                { id: 0, status: 'FOLLOWER' },
                { id: 1, status: 'FOLLOWER' },
                { id: 2, status: 'LEADER' },
            ];
            for (let i = 0; i < expect.length; ++i) {
                const { id, status } = expect[i]!;
                const server = servers[i]!;
                assert(server.config.id === id);
                assert(server.state.sm.status === status);
                console.log(`server(${server.config.id}): status=${server.state.sm.status}`);
            }
            console.log('--- finished ' + '-'.repeat(32 - 'finished'.length));
        },
    });
}

async function distruptOneServer(timeScale = 1) {
    console.log('--- distruptOneServer ' + '-'.repeat(32 - 'distruptOneServer'.length));
    await runExperiment({
        rng: new XoroRng('bar'),
        log: (raft, time, message, ...args) => {
            return log(raft, time, message, ...args);
        },
        electionInterval: [150 * timeScale, 300 * timeScale],
        heartbeatTimeout: 60 * timeScale,
        peerCount: 3,
        timestep: 1,
        maxIterations: 600 * timeScale,
        hooks: [
            (servers, time) => {
                if (time === 283) {
                    console.log('disrupting leader 2');
                    const target = servers[2]!;
                    target.state = create({ config: target.config, time }).state;
                }
            },
        ],
        finished: (servers, time) => {
            console.log('--- results ' + '-'.repeat(32 - 'results'.length));
            assert(time === 602 * timeScale);
            console.log(`time: ${time}`);
            const expect = [
                { id: 0, status: 'LEADER' },
                { id: 1, status: 'FOLLOWER' },
                { id: 2, status: 'FOLLOWER' },
            ];
            for (let i = 0; i < expect.length; ++i) {
                const { id, status } = expect[i]!;
                const server = servers[i]!;
                assert(server.config.id === id);
                assert(server.state.sm.status === status);
                console.log(`server(${server.config.id}): status=${server.state.sm.status}`);
            }
            console.log('--- finished ' + '-'.repeat(32 - 'finished'.length));
        },
    });
}

await sanityCheck();
await distruptOneServer();
