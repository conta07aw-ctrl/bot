import { RelayClient } from "../client";
import { RelayerTransaction, RelayerTransactionResponse } from "../types";
export declare class ClientRelayerTransactionResponse implements RelayerTransactionResponse {
    readonly client: RelayClient;
    readonly transactionID: string;
    readonly transactionHash: string;
    readonly hash: string;
    readonly state: string;
    constructor(transactionID: string, state: string, transactionHash: string, client: RelayClient);
    getTransaction(): Promise<RelayerTransaction[]>;
    wait(): Promise<RelayerTransaction | undefined>;
}
