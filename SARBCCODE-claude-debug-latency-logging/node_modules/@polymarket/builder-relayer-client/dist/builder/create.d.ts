import { SafeCreateTransactionArgs, TransactionRequest } from "../types";
import { IAbstractSigner } from "@polymarket/builder-abstract-signer";
import { SafeContractConfig } from "../config";
export declare function buildSafeCreateTransactionRequest(signer: IAbstractSigner, safeContractConfig: SafeContractConfig, args: SafeCreateTransactionArgs): Promise<TransactionRequest>;
