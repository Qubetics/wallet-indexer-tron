
export enum KEYS {
    latestBlock = 'latestBlock',
    behinedBlock = 'behinedBlock',
    readSpecificBlock = 'readSpecificBlock'
}
export enum Tron_contract_status {
    TransferContract = 'TransferContract',
    TriggerSmartContract = 'TriggerSmartContract',
}
export enum GlblBoolean {
    true = 1,
    false = 0
}
export enum TrnxBlockchainStatus {
    PENDING = 'pending',
    FAILED = 'failed',
    CONFIRMED = 'confirmed',
}
export enum TrnxStatusEnum {
    COMPLETED = 'completed',
}
export enum TRONSTATUS {
    SUCCESS = 'SUCCESS',
    REVERT = 'REVERT',
    OUT_OF_ENERGY = 'OUT_OF_ENERGY'
}
export enum TxReqTypesEnum {
    EXNG = 'EXNG',
}
export enum TokenStandard {
    NONE = 'null',
    ERC20 = 'erc20',  // ethereum
    ERC721 = 'erc721',  // ethereum

    BEP20 = 'bep20',  // binance
    BEP721 = 'erc721',  // binance

    TRC10 = 'trc10',  // tron
    TRC20 = 'trc20',  // tron
}
export enum BooleanEnum {
    false = '0',
    true = '1'
}
export enum CoinFamily {
    NONE = 0,
    BNB = 1,
    ETH = 2,
    BTC = 3,
    POL = 4,
    SOL = 5,
    TRON = 6
}
export enum TrnxTypeEnum {
    NONE = 'none',
    INTERNAL = 'app',
    DEPOSIT = 'deposit',
    WITHDRAW = 'withdraw',
    SMARTCONTRACTINTERACTION = 'smartcontractinteraction',
    REWARD = 'reward',
    DAPP = 'dapp',
    APPROVE = 'approve',
    SWAP = 'swap',
    CROSS_CHAIN = 'cross_chain',
    NULL = 'null',

}