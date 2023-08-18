import { create, step } from './raft.js';
import type { Time } from './time.js';
import type { LogEntry, Raft, RaftConfig, RaftMessage } from './types';

export class RaftServer<Cmd> {
    private _raft: Raft<Cmd>;
    private _inbox: RaftMessage<Cmd>[];
    private _outbox: RaftMessage<Cmd>[];
    private _receive: () => RaftMessage<Cmd> | undefined;

    constructor(public readonly time: Time, public readonly config: RaftConfig<Cmd>) {
        this._raft = create({ config, time: time.now() });
        this._inbox = [];
        this._outbox = [];
        this._receive = () => this._inbox.shift();
    }

    update(): LogEntry<Cmd>[] {
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

    send(): RaftMessage<Cmd>[] {
        const { _outbox } = this;
        this._outbox = [];
        return _outbox;
    }
}
