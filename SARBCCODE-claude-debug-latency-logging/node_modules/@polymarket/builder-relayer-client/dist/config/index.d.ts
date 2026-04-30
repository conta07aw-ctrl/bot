export interface ProxyContractConfig {
    RelayHub: string;
    ProxyFactory: string;
}
export interface SafeContractConfig {
    SafeFactory: string;
    SafeMultisend: string;
}
export interface ContractConfig {
    ProxyContracts: ProxyContractConfig;
    SafeContracts: SafeContractConfig;
}
export declare function isProxyContractConfigValid(config: ProxyContractConfig): boolean;
export declare function isSafeContractConfigValid(config: SafeContractConfig): boolean;
export declare const getContractConfig: (chainId: number) => ContractConfig;
