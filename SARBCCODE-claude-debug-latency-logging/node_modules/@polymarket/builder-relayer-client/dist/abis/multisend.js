"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.multisendAbi = void 0;
exports.multisendAbi = [
    {
        "constant": false,
        "inputs": [
            {
                "internalType": "bytes",
                "name": "transactions",
                "type": "bytes"
            }
        ],
        "name": "multiSend",
        "outputs": [],
        "payable": false,
        "stateMutability": "nonpayable",
        "type": "function"
    }
];
