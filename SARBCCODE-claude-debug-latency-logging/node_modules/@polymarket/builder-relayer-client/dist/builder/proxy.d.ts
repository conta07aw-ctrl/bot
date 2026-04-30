import { IAbstractSigner } from "@polymarket/builder-abstract-signer";
import { ProxyTransactionArgs, TransactionRequest } from "../types";
import { ProxyContractConfig } from "../config";
export declare function buildProxyTransactionRequest(signer: IAbstractSigner, args: ProxyTransactionArgs, proxyContractConfig: ProxyContractConfig, metadata?: string): Promise<TransactionRequest>;
