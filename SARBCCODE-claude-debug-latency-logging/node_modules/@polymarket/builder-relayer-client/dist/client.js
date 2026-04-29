"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.RelayClient = void 0;
const viem_1 = require("viem");
const builder_abstract_signer_1 = require("@polymarket/builder-abstract-signer");
const http_helpers_1 = require("./http-helpers");
const types_1 = require("./types");
const endpoints_1 = require("./endpoints");
const builder_1 = require("./builder");
const utils_1 = require("./utils");
const response_1 = require("./response");
const config_1 = require("./config");
const errors_1 = require("./errors");
const encode_1 = require("./encode");
class RelayClient {
    relayerUrl;
    chainId;
    relayTxType;
    contractConfig;
    httpClient;
    signer;
    builderConfig;
    constructor(relayerUrl, chainId, signer, builderConfig, relayTxType) {
        this.relayerUrl = relayerUrl.endsWith("/") ? relayerUrl.slice(0, -1) : relayerUrl;
        this.chainId = chainId;
        if (relayTxType == undefined) {
            relayTxType = types_1.RelayerTxType.SAFE;
        }
        this.relayTxType = relayTxType;
        this.contractConfig = (0, config_1.getContractConfig)(chainId);
        this.httpClient = new http_helpers_1.HttpClient();
        if (signer != undefined) {
            this.signer = (0, builder_abstract_signer_1.createAbstractSigner)(chainId, signer);
        }
        if (builderConfig !== undefined) {
            this.builderConfig = builderConfig;
        }
    }
    async getNonce(signerAddress, signerType) {
        return this.send(`${endpoints_1.GET_NONCE}`, http_helpers_1.GET, { params: { address: signerAddress, type: signerType } });
    }
    async getRelayPayload(signerAddress, signerType) {
        return this.send(`${endpoints_1.GET_RELAY_PAYLOAD}`, http_helpers_1.GET, { params: { address: signerAddress, type: signerType } });
    }
    async getTransaction(transactionId) {
        return this.send(`${endpoints_1.GET_TRANSACTION}`, http_helpers_1.GET, { params: { id: transactionId } });
    }
    async getTransactions() {
        return this.sendAuthedRequest(http_helpers_1.GET, endpoints_1.GET_TRANSACTIONS);
    }
    /**
     * Executes a batch of transactions
     * @param txns
     * @param metadata
     * @returns
     */
    async execute(txns, metadata) {
        this.signerNeeded();
        if (txns.length == 0) {
            throw new Error("no transactions to execute");
        }
        switch (this.relayTxType) {
            case types_1.RelayerTxType.SAFE:
                return this.executeSafeTransactions(txns.map(txn => ({
                    to: txn.to,
                    operation: types_1.OperationType.Call,
                    data: txn.data,
                    value: "0",
                })), metadata);
            case types_1.RelayerTxType.PROXY:
                return this.executeProxyTransactions(txns.map(txn => ({
                    to: txn.to,
                    typeCode: types_1.CallType.Call,
                    data: txn.data,
                    value: "0",
                })), metadata);
            default:
                throw new Error(`Unsupported relay transaction type: ${this.relayTxType}`);
        }
    }
    async executeProxyTransactions(txns, metadata) {
        this.signerNeeded();
        console.log(`Executing proxy transactions...`);
        const start = Date.now();
        const from = await this.signer.getAddress();
        const rp = await this.getRelayPayload(from, types_1.TransactionType.PROXY);
        const args = {
            from: from,
            gasPrice: "0",
            data: (0, encode_1.encodeProxyTransactionData)(txns),
            relay: rp.address,
            nonce: rp.nonce,
        };
        const proxyContractConfig = this.contractConfig.ProxyContracts;
        if (!(0, config_1.isProxyContractConfigValid)(proxyContractConfig)) {
            throw errors_1.CONFIG_UNSUPPORTED_ON_CHAIN;
        }
        const request = await (0, builder_1.buildProxyTransactionRequest)(this.signer, args, proxyContractConfig, metadata);
        console.log(`Client side proxy request creation took: ${(Date.now() - start) / 1000} seconds`);
        const requestPayload = JSON.stringify(request);
        const resp = await this.sendAuthedRequest(http_helpers_1.POST, endpoints_1.SUBMIT_TRANSACTION, requestPayload);
        return new response_1.ClientRelayerTransactionResponse(resp.transactionID, resp.state, resp.transactionHash, this);
    }
    async executeSafeTransactions(txns, metadata) {
        this.signerNeeded();
        console.log(`Executing safe transactions...`);
        const safe = await this.getExpectedSafe();
        const deployed = await this.getDeployed(safe);
        if (!deployed) {
            throw errors_1.SAFE_NOT_DEPLOYED;
        }
        const start = Date.now();
        const from = await this.signer.getAddress();
        const noncePayload = await this.getNonce(from, types_1.TransactionType.SAFE);
        const args = {
            transactions: txns,
            from,
            nonce: noncePayload.nonce,
            chainId: this.chainId,
        };
        const safeContractConfig = this.contractConfig.SafeContracts;
        if (!(0, config_1.isSafeContractConfigValid)(safeContractConfig)) {
            throw errors_1.CONFIG_UNSUPPORTED_ON_CHAIN;
        }
        const request = await (0, builder_1.buildSafeTransactionRequest)(this.signer, args, safeContractConfig, metadata);
        console.log(`Client side safe request creation took: ${(Date.now() - start) / 1000} seconds`);
        const requestPayload = JSON.stringify(request);
        const resp = await this.sendAuthedRequest(http_helpers_1.POST, endpoints_1.SUBMIT_TRANSACTION, requestPayload);
        return new response_1.ClientRelayerTransactionResponse(resp.transactionID, resp.state, resp.transactionHash, this);
    }
    /**
     * Deploys a safe
     * @returns
     */
    async deploy() {
        this.signerNeeded();
        const safe = await this.getExpectedSafe();
        const deployed = await this.getDeployed(safe);
        if (deployed) {
            throw errors_1.SAFE_DEPLOYED;
        }
        console.log(`Deploying safe ${safe}...`);
        return this._deploy();
    }
    async _deploy() {
        const start = Date.now();
        const from = await this.signer.getAddress();
        const args = {
            from: from,
            chainId: this.chainId,
            paymentToken: viem_1.zeroAddress,
            payment: "0",
            paymentReceiver: viem_1.zeroAddress,
        };
        const safeContractConfig = this.contractConfig.SafeContracts;
        const request = await (0, builder_1.buildSafeCreateTransactionRequest)(this.signer, safeContractConfig, args);
        console.log(`Client side deploy request creation took: ${(Date.now() - start) / 1000} seconds`);
        const requestPayload = JSON.stringify(request);
        const resp = await this.sendAuthedRequest(http_helpers_1.POST, endpoints_1.SUBMIT_TRANSACTION, requestPayload);
        return new response_1.ClientRelayerTransactionResponse(resp.transactionID, resp.state, resp.transactionHash, this);
    }
    async getDeployed(safe) {
        const resp = await this.send(`${endpoints_1.GET_DEPLOYED}`, http_helpers_1.GET, { params: { address: safe } });
        return resp.deployed;
    }
    /**
     * Periodically polls the transaction id until it reaches a desired state
     * Returns the relayer transaction if it does each the desired state
     * Returns undefined if the transaction hits the failed state
     * Times out after maxPolls is reached
     * @param transactionId
     * @param states
     * @param failState
     * @param maxPolls
     * @param pollFrequency
     * @returns
     */
    async pollUntilState(transactionId, states, failState, maxPolls, pollFrequency) {
        console.log(`Waiting for transaction ${transactionId} matching states: ${states}...`);
        const maxPollCount = maxPolls != undefined ? maxPolls : 10;
        let pollFreq = 2000; // Default to polling every 2 seconds
        if (pollFrequency != undefined) {
            if (pollFrequency >= 1000) {
                pollFreq = pollFrequency;
            }
        }
        let pollCount = 0;
        while (pollCount < maxPollCount) {
            const txns = await this.getTransaction(transactionId);
            if (txns.length > 0) {
                const txn = txns[0];
                if (states.includes(txn.state)) {
                    return txn;
                }
                if (failState != undefined && txn.state == failState) {
                    console.error(`txn ${transactionId} failed onchain! Transaction hash: ${txn.transactionHash}`);
                    return undefined;
                }
            }
            pollCount++;
            await (0, utils_1.sleep)(pollFreq);
        }
        console.log(`Transaction not found or not in given states, timing out!`);
    }
    async sendAuthedRequest(method, path, body) {
        // builders auth
        if (this.canBuilderAuth()) {
            const builderHeaders = await this._generateBuilderHeaders(method, path, body);
            if (builderHeaders !== undefined) {
                return this.send(path, method, { headers: builderHeaders, data: body });
            }
        }
        return this.send(path, method, { data: body });
    }
    async _generateBuilderHeaders(method, path, body) {
        if (this.builderConfig !== undefined) {
            const builderHeaders = await this.builderConfig.generateBuilderHeaders(method, path, body);
            if (builderHeaders == undefined) {
                return undefined;
            }
            return builderHeaders;
        }
        return undefined;
    }
    canBuilderAuth() {
        return (this.builderConfig != undefined && this.builderConfig.isValid());
    }
    async send(endpoint, method, options) {
        const resp = await this.httpClient.send(`${this.relayerUrl}${endpoint}`, method, options);
        return resp.data;
    }
    signerNeeded() {
        if (this.signer === undefined) {
            throw errors_1.SIGNER_UNAVAILABLE;
        }
    }
    async getExpectedSafe() {
        const address = await this.signer.getAddress();
        return (0, builder_1.deriveSafe)(address, this.contractConfig.SafeContracts.SafeFactory);
    }
}
exports.RelayClient = RelayClient;
