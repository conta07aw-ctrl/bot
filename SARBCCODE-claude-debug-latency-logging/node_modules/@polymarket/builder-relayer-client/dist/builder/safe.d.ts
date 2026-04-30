import { IAbstractSigner } from "@polymarket/builder-abstract-signer";
import { SafeTransaction, SafeTransactionArgs, TransactionRequest } from "../types";
import { SafeContractConfig } from "../config";
export declare function aggregateTransaction(txns: SafeTransaction[], safeMultisend: string): SafeTransaction;
export declare function buildSafeTransactionRequest(signer: IAbstractSigner, args: SafeTransactionArgs, safeContractConfig: SafeContractConfig, metadata?: string): Promise<TransactionRequest>;
