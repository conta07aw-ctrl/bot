"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.createSafeMultisendTransaction = void 0;
const viem_1 = require("viem");
const abis_1 = require("../abis");
const types_1 = require("../types");
const multisend = (0, viem_1.prepareEncodeFunctionData)({
    abi: abis_1.multisendAbi,
    functionName: "multiSend",
});
const createSafeMultisendTransaction = (txns, safeMultisendAddress) => {
    const args = [
        (0, viem_1.concatHex)(txns.map(tx => (0, viem_1.encodePacked)(["uint8", "address", "uint256", "uint256", "bytes"], [tx.operation, tx.to, BigInt(tx.value), BigInt((0, viem_1.size)(tx.data)), tx.data]))),
    ];
    const data = (0, viem_1.encodeFunctionData)({ ...multisend, args: args });
    return {
        to: safeMultisendAddress,
        value: "0",
        data: data,
        operation: types_1.OperationType.DelegateCall,
    };
};
exports.createSafeMultisendTransaction = createSafeMultisendTransaction;
