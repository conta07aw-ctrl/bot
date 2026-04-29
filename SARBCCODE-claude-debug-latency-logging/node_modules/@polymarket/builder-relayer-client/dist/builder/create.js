"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.buildSafeCreateTransactionRequest = buildSafeCreateTransactionRequest;
const types_1 = require("../types");
const constants_1 = require("../constants");
const derive_1 = require("./derive");
async function createSafeCreateSignature(signer, safeFactory, chainId, paymentToken, payment, paymentReceiver) {
    const domain = {
        name: constants_1.SAFE_FACTORY_NAME,
        chainId: BigInt(chainId),
        verifyingContract: safeFactory,
    };
    const types = {
        CreateProxy: [
            { name: "paymentToken", type: "address" },
            { name: "payment", type: "uint256" },
            { name: "paymentReceiver", type: "address" },
        ],
    };
    const values = {
        paymentToken,
        payment: BigInt(payment),
        paymentReceiver,
    };
    const sig = await signer.signTypedData(domain, types, values, "CreateProxy");
    console.log(`Sig: ${sig}`);
    return sig;
}
async function buildSafeCreateTransactionRequest(signer, safeContractConfig, args) {
    const safeFactory = safeContractConfig.SafeFactory;
    const sig = await createSafeCreateSignature(signer, safeFactory, args.chainId, args.paymentToken, args.payment, args.paymentReceiver);
    const sigParams = {
        paymentToken: args.paymentToken,
        payment: args.payment,
        paymentReceiver: args.paymentReceiver,
    };
    const safeAddress = (0, derive_1.deriveSafe)(args.from, safeFactory);
    const request = {
        from: args.from,
        to: safeFactory,
        // Note: obviously the safe here does not exist yet but useful to have this data in the db
        proxyWallet: safeAddress,
        data: "0x",
        signature: sig,
        signatureParams: sigParams,
        type: types_1.TransactionType.SAFE_CREATE,
    };
    console.log(`Created a SAFE-CREATE Transaction:`);
    console.log(request);
    return request;
}
