export interface Time {
    now(): number;
}

export class DeterministicTime {
    private _time: number;

    constructor(time = 0) {
        this._time = time;
    }

    step(amount: number): void {
        this._time += amount;
    }

    now(): number {
        return this._time++;
    }
}
