"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.ClientRelayerTransactionResponse = void 0;
const types_1 = require("../types");
class ClientRelayerTransactionResponse {
    client;
    transactionID;
    transactionHash;
    hash;
    state;
    constructor(transactionID, state, transactionHash, client) {
        this.transactionID = transactionID;
        this.state = state;
        this.transactionHash = transactionHash;
        this.hash = transactionHash;
        this.client = client;
    }
    async getTransaction() {
        return this.client.getTransaction(this.transactionID);
    }
    async wait() {
        return this.client.pollUntilState(this.transactionID, [
            types_1.RelayerTransactionState.STATE_MINED,
            types_1.RelayerTransactionState.STATE_CONFIRMED,
        ], types_1.RelayerTransactionState.STATE_FAILED, 100);
    }
}
exports.ClientRelayerTransactionResponse = ClientRelayerTransactionResponse;
