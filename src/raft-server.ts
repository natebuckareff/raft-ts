import { create, step } from './raft.js';
import type { Time } from './time.js';
import type { LogEntry, Raft, RaftConfig, RaftMessage } from './types';

export class RaftServer {
    private _raft: Raft;
    private _inbox: RaftMessage[];
    private _outbox: RaftMessage[];
    private _receive: () => RaftMessage | undefined;

    constructor(public readonly time: Time, public readonly config: RaftConfig) {
        this._raft = create({ config, time: time.now() });
        this._inbox = [];
        this._outbox = [];
        this._receive = () => this._inbox.shift();
    }

    update(): LogEntry[] {
        const result = step(this._raft, this.time.now(), this._receive);

        if (result !== undefined) {
            if (result.type === 'SEND') {
                if ('message' in result) {
                    this._outbox.push(result.message);
                } else {
                    this._outbox.push(...result.messages);
                }
            } else {
                return result.entries;
            }
        }

        return [];
    }

    send(): RaftMessage[] {
        const { _outbox } = this;
        this._outbox = [];
        return _outbox;
    }
}
