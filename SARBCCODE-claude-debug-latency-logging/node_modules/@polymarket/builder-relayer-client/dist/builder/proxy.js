"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.buildProxyTransactionRequest = buildProxyTransactionRequest;
const viem_1 = require("viem");
const types_1 = require("../types");
const derive_1 = require("./derive");
const DEFAULT_GAS_LIMIT = BigInt(10_000_000);
function createStructHash(from, to, data, txFee, gasPrice, gasLimit, nonce, relayHubAddress, relayAddress) {
    const relayHubPrefix = (0, viem_1.toHex)("rlx:");
    const encodedFrom = from;
    const encodedTo = to;
    const encodedData = data;
    const encodedTxFee = (0, viem_1.toHex)(BigInt(txFee), { size: 32 });
    const encodedGasPrice = (0, viem_1.toHex)(BigInt(gasPrice), { size: 32 });
    const encodedGasLimit = (0, viem_1.toHex)(BigInt(gasLimit), { size: 32 });
    const encodedNonce = (0, viem_1.toHex)(BigInt(nonce), { size: 32 });
    const encodedRelayHubAddress = relayHubAddress;
    const encodedRelayAddress = relayAddress;
    const dataToHash = (0, viem_1.concat)([
        relayHubPrefix,
        encodedFrom,
        encodedTo,
        encodedData,
        encodedTxFee,
        encodedGasPrice,
        encodedGasLimit,
        encodedNonce,
        encodedRelayHubAddress,
        encodedRelayAddress,
    ]);
    return (0, viem_1.keccak256)(dataToHash);
}
async function createProxySignature(signer, structHash) {
    return signer.signMessage(structHash);
}
async function buildProxyTransactionRequest(signer, args, proxyContractConfig, metadata) {
    const proxyWalletFactory = proxyContractConfig.ProxyFactory;
    const to = proxyWalletFactory;
    const proxy = (0, derive_1.deriveProxyWallet)(args.from, proxyWalletFactory);
    const relayerFee = "0";
    const relayHub = proxyContractConfig.RelayHub;
    const gasLimitStr = await getGasLimit(signer, to, args);
    const sigParams = {
        gasPrice: args.gasPrice,
        gasLimit: gasLimitStr,
        relayerFee: relayerFee,
        relayHub: relayHub,
        relay: args.relay,
    };
    const txHash = createStructHash(args.from, to, args.data, relayerFee, args.gasPrice, gasLimitStr, args.nonce, relayHub, args.relay);
    const sig = await createProxySignature(signer, txHash);
    if (metadata == undefined) {
        metadata = "";
    }
    const req = {
        from: args.from,
        to: to,
        proxyWallet: proxy,
        data: args.data,
        nonce: args.nonce,
        signature: sig,
        signatureParams: sigParams,
        type: types_1.TransactionType.PROXY,
        metadata: metadata,
    };
    console.log(`Created Proxy Transaction Request:`);
    console.log(req);
    return req;
}
async function getGasLimit(signer, to, args) {
    if (args.gasLimit && args.gasLimit !== "0") {
        return args.gasLimit;
    }
    let gasLimitBigInt;
    try {
        gasLimitBigInt = await signer.estimateGas({
            from: args.from,
            to: to,
            data: args.data,
        });
    }
    catch (e) {
        console.log("Error estimating gas for proxy transaction, using default gas limit:", e);
        gasLimitBigInt = DEFAULT_GAS_LIMIT;
    }
    return gasLimitBigInt.toString();
}
