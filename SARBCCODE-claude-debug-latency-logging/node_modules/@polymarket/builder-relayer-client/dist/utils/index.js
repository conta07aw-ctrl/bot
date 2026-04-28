"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.splitAndPackSig = splitAndPackSig;
exports.sleep = sleep;
const viem_1 = require("viem");
function splitAndPackSig(sig) {
    const splitSig = splitSignature(sig);
    const packedSig = (0, viem_1.encodePacked)(["uint256", "uint256", "uint8"], [BigInt(splitSig.r), BigInt(splitSig.s), parseInt(splitSig.v)]);
    return packedSig;
}
function splitSignature(sig) {
    let sigV = parseInt(sig.slice(-2), 16);
    switch (sigV) {
        case 0:
        case 1:
            sigV += 31;
            break;
        case 27:
        case 28:
            sigV += 4;
            break;
        default:
            throw new Error("Invalid signature");
    }
    sig = sig.slice(0, -2) + sigV.toString(16);
    return {
        r: (0, viem_1.hexToBigInt)('0x' + sig.slice(2, 66)).toString(),
        s: (0, viem_1.hexToBigInt)('0x' + sig.slice(66, 130)).toString(),
        v: (0, viem_1.hexToBigInt)('0x' + sig.slice(130, 132)).toString(),
    };
}
function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}
