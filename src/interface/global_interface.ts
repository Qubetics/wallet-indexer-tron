export interface blocks_trnx {
    block_number: number,
    tx_id: string,
    retry_count: number,
    key: string | null,
    pair: string | null
}
export interface transaction_data {
    status: string,
    block_number: number,
    tx_id: string,
    contract: any[],
    retry_count: number,
    key: string | null,
    pair: string | null
}

export interface Transaction {
    tx_id: string;
    fromAddress: string;
    toAddress: string;
    token: string | null;
    amount: number;
    blockId: number;
    isNFTToken: boolean;
    txType: string | null;
    trnx_fee: number;
}
