"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.CONFIG_UNSUPPORTED_ON_CHAIN = exports.SAFE_NOT_DEPLOYED = exports.SAFE_DEPLOYED = exports.SIGNER_UNAVAILABLE = void 0;
exports.SIGNER_UNAVAILABLE = new Error("signer is needed to interact with this endpoint!");
exports.SAFE_DEPLOYED = new Error("safe already deployed!");
exports.SAFE_NOT_DEPLOYED = new Error("safe not deployed!");
exports.CONFIG_UNSUPPORTED_ON_CHAIN = new Error("config is not supported on the chainId");
