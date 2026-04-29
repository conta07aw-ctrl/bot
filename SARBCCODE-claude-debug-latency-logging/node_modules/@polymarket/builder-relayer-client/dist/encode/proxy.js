"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.encodeProxyTransactionData = encodeProxyTransactionData;
const abis_1 = require("../abis");
const viem_1 = require("viem");
const proxy = (0, viem_1.prepareEncodeFunctionData)({
    abi: abis_1.proxyWalletFactory,
    functionName: 'proxy',
});
function encodeProxyTransactionData(txns) {
    return (0, viem_1.encodeFunctionData)({
        ...proxy,
        args: [txns],
    });
}
