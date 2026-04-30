export interface SplitSig {
    r: string;
    s: string;
    v: string;
}
export declare function splitAndPackSig(sig: string): string;
export declare function sleep(ms: number): Promise<unknown>;
