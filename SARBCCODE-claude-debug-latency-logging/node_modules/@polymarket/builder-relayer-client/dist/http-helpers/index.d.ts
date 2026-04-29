import { AxiosInstance, AxiosRequestHeaders, AxiosResponse } from "axios";
export declare const GET = "GET";
export declare const POST = "POST";
export declare const DELETE = "DELETE";
export declare const PUT = "PUT";
export type QueryParams = Record<string, any>;
export interface RequestOptions {
    headers?: AxiosRequestHeaders;
    data?: any;
    params?: QueryParams;
}
export declare class HttpClient {
    readonly instance: AxiosInstance;
    constructor();
    send(endpoint: string, method: string, options?: RequestOptions): Promise<AxiosResponse>;
}
