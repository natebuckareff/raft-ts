export interface Time {
    now(): number;
}

export class DeterministicTime {
    private _time: number;

    constructor(time = 0) {
        this._time = time;
    }

    now(): number {
        return this._time++;
    }
}
