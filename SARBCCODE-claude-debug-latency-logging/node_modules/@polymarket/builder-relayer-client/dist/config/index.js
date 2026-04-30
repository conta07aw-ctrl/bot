"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.getContractConfig = void 0;
exports.isProxyContractConfigValid = isProxyContractConfigValid;
exports.isSafeContractConfigValid = isSafeContractConfigValid;
;
const AMOY = {
    ProxyContracts: {
        // Proxy factory unsupported on Amoy testnet
        RelayHub: "",
        ProxyFactory: "",
    },
    SafeContracts: {
        SafeFactory: "0xaacFeEa03eb1561C4e67d661e40682Bd20E3541b",
        SafeMultisend: "0xA238CBeb142c10Ef7Ad8442C6D1f9E89e07e7761",
    }
};
const POL = {
    ProxyContracts: {
        ProxyFactory: "0xaB45c5A4B0c941a2F231C04C3f49182e1A254052",
        RelayHub: "0xD216153c06E857cD7f72665E0aF1d7D82172F494"
    },
    SafeContracts: {
        SafeFactory: "0xaacFeEa03eb1561C4e67d661e40682Bd20E3541b",
        SafeMultisend: "0xA238CBeb142c10Ef7Ad8442C6D1f9E89e07e7761",
    }
};
function isProxyContractConfigValid(config) {
    return !!config.RelayHub && !!config.ProxyFactory;
}
function isSafeContractConfigValid(config) {
    return !!config.SafeFactory && !!config.SafeMultisend;
}
const getContractConfig = (chainId) => {
    switch (chainId) {
        case 137:
            return POL;
        case 80002:
            return AMOY;
        default:
            throw new Error("Invalid network");
    }
};
exports.getContractConfig = getContractConfig;
