"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.HttpClient = exports.PUT = exports.DELETE = exports.POST = exports.GET = void 0;
const tslib_1 = require("tslib");
const axios_1 = tslib_1.__importDefault(require("axios"));
exports.GET = "GET";
exports.POST = "POST";
exports.DELETE = "DELETE";
exports.PUT = "PUT";
class HttpClient {
    instance;
    constructor() {
        this.instance = axios_1.default.create({ withCredentials: true });
    }
    async send(endpoint, method, options) {
        if (options !== undefined) {
            if (options.headers != undefined) {
                options.headers["Access-Control-Allow-Credentials"] = true;
            }
        }
        try {
            const resp = await this.instance.request({
                url: endpoint,
                method: method,
                headers: options?.headers,
                data: options?.data,
                params: options?.params,
            });
            return resp;
        }
        catch (err) {
            if (axios_1.default.isAxiosError(err)) {
                if (err.response) {
                    const errPayload = {
                        error: "request error",
                        status: err.response?.status,
                        statusText: err.response?.statusText,
                        data: err.response?.data,
                    };
                    console.error("request error", errPayload);
                    throw new Error(JSON.stringify(errPayload));
                }
                else {
                    const errPayload = { error: "connection error" };
                    console.error("connection error", errPayload);
                    throw new Error(JSON.stringify(errPayload));
                }
            }
            throw new Error(JSON.stringify({ error: err }));
        }
    }
}
exports.HttpClient = HttpClient;
