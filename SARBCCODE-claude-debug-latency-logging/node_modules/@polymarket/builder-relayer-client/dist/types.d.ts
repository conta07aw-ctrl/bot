export declare enum RelayerTxType {
    SAFE = "SAFE",
    PROXY = "PROXY"
}
export declare enum TransactionType {
    SAFE = "SAFE",
    PROXY = "PROXY",
    SAFE_CREATE = "SAFE-CREATE"
}
export interface SignatureParams {
    gasPrice?: string;
    relayerFee?: string;
    gasLimit?: string;
    relayHub?: string;
    relay?: string;
    operation?: string;
    safeTxnGas?: string;
    baseGas?: string;
    gasToken?: string;
    refundReceiver?: string;
    paymentToken?: string;
    payment?: string;
    paymentReceiver?: string;
}
export interface AddressPayload {
    address: string;
}
export interface NoncePayload {
    nonce: string;
}
export interface RelayPayload {
    address: string;
    nonce: string;
}
export interface TransactionRequest {
    type: string;
    from: string;
    to: string;
    proxyWallet?: string;
    data: string;
    nonce?: string;
    signature: string;
    signatureParams: SignatureParams;
    metadata?: string;
}
export declare enum CallType {
    Invalid = "0",
    Call = "1",
    DelegateCall = "2"
}
export interface ProxyTransaction {
    to: string;
    typeCode: CallType;
    data: string;
    value: string;
}
export declare enum OperationType {
    Call = 0,// 0
    DelegateCall = 1
}
export interface SafeTransaction {
    to: string;
    operation: OperationType;
    data: string;
    value: string;
}
export interface Transaction {
    to: string;
    data: string;
    value: string;
}
export interface SafeTransactionArgs {
    from: string;
    nonce: string;
    chainId: number;
    transactions: SafeTransaction[];
}
export interface SafeCreateTransactionArgs {
    from: string;
    chainId: number;
    paymentToken: string;
    payment: string;
    paymentReceiver: string;
}
export interface ProxyTransactionArgs {
    from: string;
    nonce: string;
    gasPrice: string;
    gasLimit?: string;
    data: string;
    relay: string;
}
export declare enum RelayerTransactionState {
    STATE_NEW = "STATE_NEW",
    STATE_EXECUTED = "STATE_EXECUTED",
    STATE_MINED = "STATE_MINED",
    STATE_INVALID = "STATE_INVALID",
    STATE_CONFIRMED = "STATE_CONFIRMED",
    STATE_FAILED = "STATE_FAILED"
}
export interface RelayerTransaction {
    transactionID: string;
    transactionHash: string;
    from: string;
    to: string;
    proxyAddress: string;
    data: string;
    nonce: string;
    value: string;
    state: string;
    type: string;
    metadata: string;
    createdAt: Date;
    updatedAt: Date;
}
export interface RelayerTransactionResponse {
    transactionID: string;
    state: string;
    hash: string;
    transactionHash: string;
    getTransaction: () => Promise<RelayerTransaction[]>;
    wait: () => Promise<RelayerTransaction | undefined>;
}
export interface GetDeployedResponse {
    deployed: boolean;
}
