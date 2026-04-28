import { Wallet } from "@ethersproject/wallet";
import { JsonRpcSigner } from "@ethersproject/providers";
import { WalletClient } from "viem";
import { IAbstractSigner } from "@polymarket/builder-abstract-signer";
import { HttpClient } from "./http-helpers";
import { NoncePayload, RelayerTransaction, RelayerTransactionResponse, RelayerTxType, RelayPayload, Transaction } from "./types";
import { ContractConfig } from "./config";
import { BuilderConfig } from "@polymarket/builder-signing-sdk";
export declare class RelayClient {
    readonly relayerUrl: string;
    readonly chainId: number;
    readonly relayTxType: RelayerTxType;
    readonly contractConfig: ContractConfig;
    readonly httpClient: HttpClient;
    readonly signer?: IAbstractSigner;
    readonly builderConfig?: BuilderConfig;
    constructor(relayerUrl: string, chainId: number, signer?: Wallet | JsonRpcSigner | WalletClient, builderConfig?: BuilderConfig, relayTxType?: RelayerTxType);
    getNonce(signerAddress: string, signerType: string): Promise<NoncePayload>;
    getRelayPayload(signerAddress: string, signerType: string): Promise<RelayPayload>;
    getTransaction(transactionId: string): Promise<RelayerTransaction[]>;
    getTransactions(): Promise<RelayerTransaction[]>;
    /**
     * Executes a batch of transactions
     * @param txns
     * @param metadata
     * @returns
     */
    execute(txns: Transaction[], metadata?: string): Promise<RelayerTransactionResponse>;
    private executeProxyTransactions;
    private executeSafeTransactions;
    /**
     * Deploys a safe
     * @returns
     */
    deploy(): Promise<RelayerTransactionResponse>;
    private _deploy;
    getDeployed(safe: string): Promise<boolean>;
    /**
     * Periodically polls the transaction id until it reaches a desired state
     * Returns the relayer transaction if it does each the desired state
     * Returns undefined if the transaction hits the failed state
     * Times out after maxPolls is reached
     * @param transactionId
     * @param states
     * @param failState
     * @param maxPolls
     * @param pollFrequency
     * @returns
     */
    pollUntilState(transactionId: string, states: string[], failState?: string, maxPolls?: number, pollFrequency?: number): Promise<RelayerTransaction | undefined>;
    private sendAuthedRequest;
    private _generateBuilderHeaders;
    private canBuilderAuth;
    private send;
    private signerNeeded;
    private getExpectedSafe;
}
