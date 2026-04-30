"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.deriveSafe = exports.deriveProxyWallet = void 0;
const viem_1 = require("viem");
const constants_1 = require("../constants");
const deriveProxyWallet = (address, proxyFactory) => {
    return (0, viem_1.getCreate2Address)({
        bytecodeHash: constants_1.PROXY_INIT_CODE_HASH,
        from: proxyFactory,
        salt: (0, viem_1.keccak256)((0, viem_1.encodePacked)(["address"], [address]))
    });
};
exports.deriveProxyWallet = deriveProxyWallet;
const deriveSafe = (address, safeFactory) => {
    return (0, viem_1.getCreate2Address)({
        bytecodeHash: constants_1.SAFE_INIT_CODE_HASH,
        from: safeFactory,
        salt: (0, viem_1.keccak256)((0, viem_1.encodeAbiParameters)([{ name: 'address', type: 'address' }], [address]))
    });
};
exports.deriveSafe = deriveSafe;
