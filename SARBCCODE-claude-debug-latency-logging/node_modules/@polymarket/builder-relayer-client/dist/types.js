"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.RelayerTransactionState = exports.OperationType = exports.CallType = exports.TransactionType = exports.RelayerTxType = void 0;
var RelayerTxType;
(function (RelayerTxType) {
    RelayerTxType["SAFE"] = "SAFE";
    RelayerTxType["PROXY"] = "PROXY";
})(RelayerTxType || (exports.RelayerTxType = RelayerTxType = {}));
var TransactionType;
(function (TransactionType) {
    TransactionType["SAFE"] = "SAFE";
    TransactionType["PROXY"] = "PROXY";
    TransactionType["SAFE_CREATE"] = "SAFE-CREATE";
})(TransactionType || (exports.TransactionType = TransactionType = {}));
var CallType;
(function (CallType) {
    CallType["Invalid"] = "0";
    CallType["Call"] = "1";
    CallType["DelegateCall"] = "2";
})(CallType || (exports.CallType = CallType = {}));
// Safe Transactions
var OperationType;
(function (OperationType) {
    OperationType[OperationType["Call"] = 0] = "Call";
    OperationType[OperationType["DelegateCall"] = 1] = "DelegateCall";
})(OperationType || (exports.OperationType = OperationType = {}));
var RelayerTransactionState;
(function (RelayerTransactionState) {
    RelayerTransactionState["STATE_NEW"] = "STATE_NEW";
    RelayerTransactionState["STATE_EXECUTED"] = "STATE_EXECUTED";
    RelayerTransactionState["STATE_MINED"] = "STATE_MINED";
    RelayerTransactionState["STATE_INVALID"] = "STATE_INVALID";
    RelayerTransactionState["STATE_CONFIRMED"] = "STATE_CONFIRMED";
    RelayerTransactionState["STATE_FAILED"] = "STATE_FAILED";
})(RelayerTransactionState || (exports.RelayerTransactionState = RelayerTransactionState = {}));
