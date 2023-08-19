import assert from 'assert';
import type { RNG } from '../rng';

export interface Task<T> {
    id: number;
    inbox: T[];
    generator: Generator<T | undefined, void>;
}

export interface ReceiveFn<T> {
    (match?: (message: T) => boolean): T | undefined;
}

export interface TaskFactory<T> {
    (id: number, receive: ReceiveFn<T>): Generator<T | undefined, void>;
}

export interface MessageRouter<T> {
    (sched: TaskScheduler<T>, message: T): number;
}

export interface SchedulerStrategy<T> {
    (sched: TaskScheduler<T>): number | undefined;
}

export interface SchedulerMetrics {
    running: number[];
    spawned: number[];
    killed: number[];
}

export interface TaskSchedulerConfig<T> {
    router: MessageRouter<T>;
    strategy: SchedulerStrategy<T>;
}

export class TaskScheduler<T> {
    private _ids: number;
    private _tasks: Map<number, Task<T>>;
    private _metrics: SchedulerMetrics;

    constructor(public readonly config: TaskSchedulerConfig<T>) {
        this._ids = 0;
        this._tasks = new Map();
        this._metrics = { running: [], spawned: [], killed: [] };
    }

    get metrics(): Readonly<SchedulerMetrics> {
        return this._metrics;
    }

    spawn(factory: TaskFactory<T>): number {
        const id = this._ids++;
        const task: Task<T> = {
            id,
            inbox: [],
            generator: undefined!,
        };

        const receive: ReceiveFn<T> = match => {
            if (match === undefined) {
                return task.inbox.shift();
            } else {
                for (let i = 0; i < task.inbox.length; ++i) {
                    const msg = task.inbox[i]!;
                    if (match(msg)) {
                        task.inbox.splice(i, 1);
                        return msg;
                    }
                }
                return;
            }
        };

        task.generator = factory(id, receive);

        this._tasks.set(id, task);
        this._metrics.spawned.push(id);

        return id;
    }

    kill(id: number): boolean {
        this._tasks.delete(id);
        this._metrics.killed.push(id);
        this._metrics.running = this._metrics.running.filter(x => x !== id);
        return false;
    }

    run(): boolean {
        const id = this.config.strategy(this);

        this._metrics.running.push(...this._metrics.spawned);
        this._metrics.spawned = [];
        this._metrics.killed = [];

        if (id !== undefined) {
            const task = this._tasks.get(id);
            assert(task !== undefined);
            const result = task.generator.next();

            if (result.done === true) {
                this.kill(id);
            } else {
                const message = result.value;
                if (message !== undefined) {
                    const to = this.config.router(this, message);
                    this._tasks.get(to)?.inbox.push(message);
                }
            }
            return true;
        }

        return false;
    }
}

export const createRoundRobinStrategy = <T>(): SchedulerStrategy<T> => {
    let queue: number[] = [];
    return sched => {
        queue = queue.filter(id => !sched.metrics.killed.includes(id));
        queue.push(...sched.metrics.spawned);
        const id = queue.shift();
        if (id !== undefined) {
            queue.push(id);
        }
        return id;
    };
};

export const createRandomStrategy = <T>(rng: RNG): SchedulerStrategy<T> => {
    let ids: number[] = [];
    return sched => {
        ids = ids.filter(id => !sched.metrics.killed.includes(id));
        ids.push(...sched.metrics.spawned);
        const i = Math.floor(rng.random() * ids.length);
        return ids[i];
    };
};
