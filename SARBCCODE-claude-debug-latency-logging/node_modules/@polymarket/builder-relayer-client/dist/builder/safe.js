"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.aggregateTransaction = aggregateTransaction;
exports.buildSafeTransactionRequest = buildSafeTransactionRequest;
const viem_1 = require("viem");
const types_1 = require("../types");
const derive_1 = require("./derive");
const safe_1 = require("../encode/safe");
const utils_1 = require("../utils");
async function createSafeSignature(signer, structHash) {
    return signer.signMessage(structHash);
}
function createStructHash(chainId, safe, to, value, data, operation, safeTxGas, baseGas, gasPrice, gasToken, refundReceiver, nonce) {
    const domain = {
        chainId: chainId,
        verifyingContract: safe,
    };
    const types = {
        // keccak256(
        //     "SafeTx(address to,uint256 value,bytes data,uint8 operation,uint256 safeTxGas,uint256 baseGas,uint256 gasPrice,address gasToken,address refundReceiver,uint256 nonce)"
        // );
        SafeTx: [
            { name: 'to', type: 'address' },
            { name: 'value', type: 'uint256' },
            { name: 'data', type: 'bytes' },
            { name: 'operation', type: 'uint8' },
            { name: 'safeTxGas', type: 'uint256' },
            { name: 'baseGas', type: 'uint256' },
            { name: 'gasPrice', type: 'uint256' },
            { name: 'gasToken', type: 'address' },
            { name: 'refundReceiver', type: 'address' },
            { name: 'nonce', type: 'uint256' },
        ],
    };
    const values = {
        to: to,
        value: value,
        data: data,
        operation: operation,
        safeTxGas: safeTxGas,
        baseGas: baseGas,
        gasPrice: gasPrice,
        gasToken: gasToken,
        refundReceiver: refundReceiver,
        nonce: nonce,
    };
    // // viem hashTypedData
    // const structHash = _TypedDataEncoder.hash(domain, types, values);
    const structHash = (0, viem_1.hashTypedData)({ primaryType: "SafeTx", domain: domain, types: types, message: values });
    return structHash;
}
function aggregateTransaction(txns, safeMultisend) {
    let transaction;
    if (txns.length == 1) {
        transaction = txns[0];
    }
    else {
        transaction = (0, safe_1.createSafeMultisendTransaction)(txns, safeMultisend);
    }
    return transaction;
}
async function buildSafeTransactionRequest(signer, args, safeContractConfig, metadata) {
    const safeFactory = safeContractConfig.SafeFactory;
    const safeMultisend = safeContractConfig.SafeMultisend;
    const transaction = aggregateTransaction(args.transactions, safeMultisend);
    const safeTxnGas = "0";
    const baseGas = "0";
    const gasPrice = "0";
    const gasToken = viem_1.zeroAddress;
    const refundReceiver = viem_1.zeroAddress;
    const safeAddress = (0, derive_1.deriveSafe)(args.from, safeFactory);
    // Generate the struct hash
    const structHash = createStructHash(args.chainId, safeAddress, transaction.to, transaction.value, transaction.data, transaction.operation, safeTxnGas, baseGas, gasPrice, gasToken, refundReceiver, args.nonce);
    const sig = await createSafeSignature(signer, structHash);
    // Split the sig then pack it into Gnosis accepted rsv format
    const packedSig = (0, utils_1.splitAndPackSig)(sig);
    const sigParams = {
        gasPrice,
        operation: `${transaction.operation}`,
        safeTxnGas,
        baseGas,
        gasToken,
        refundReceiver,
    };
    if (metadata == undefined) {
        metadata = "";
    }
    const req = {
        from: args.from,
        to: transaction.to,
        proxyWallet: safeAddress,
        data: transaction.data,
        nonce: args.nonce,
        signature: packedSig,
        signatureParams: sigParams,
        type: types_1.TransactionType.SAFE,
        metadata: metadata,
    };
    console.log(`Created Safe Transaction Request: `);
    console.log(req);
    return req;
}
