import { config } from "../config";
import { CoinFamily } from "../enum";

import { global_helper, kafkaHelper, wallet_queries } from "../helpers";
import { CoinsModel, WalletModel } from "../models/model";
import tronProcessHelper from "./processes.helper";



class UpdateBalanceController {

    public trx_pending_balance_update_topic: any = config.KAFKA_BALANCE.TRX_PENDING_BALANCE_UPDATE_TOPIC;
    public trx_pending_balance_update_group: any = config.KAFKA_BALANCE.TRX_PENDING_BALANCE_UPDATE_GROUP_ID;
    public update_user_all_coins_balance_topic: any = config.KAFKA_BALANCE.UPDATE_USER_ALL_TRX_COINS_BALANCE_TOPIC;
    public update_user_all_coins_balance_group_id: any = config.KAFKA_BALANCE.UPDATE_USER_ALL_TRX_COINS_BALANCE_GROUP_ID;

    public startUpdateBalance = async () => {
        let consumer = await kafkaHelper.kafka.consumer({ groupId: this.trx_pending_balance_update_group });
        await consumer.subscribe({ topic: this.trx_pending_balance_update_topic, fromBeginning: true });
        await consumer.run({
            eachMessage: async ({ topic, partition, message }: { topic: any, partition: any, message: any }) => {
                // console.log("🚀 ~ startTxStatusUpdateQueue topic >>>>", topic);
                // console.log("🚀 ~ startTxStatusUpdateQueue message.value >>>>", message.value.toString());
                await this.userBalance(JSON.parse(message.value.toString()))
            }
        }).catch((err: Error) => {
            console.log("🔥 ~ ~ startTxStatusUpdateQueue error", err);
        });
    };

    public async userBalance(data: any) {
        try {
            if (data) {
                if (data.coin_data) {
                    if (data.queue_count > 5) {
                        console.log(`Balance update attempts exceeded (5) for address: ${data.wallet_address}`);
                        return;
                    }
                    const coinData = data.coin_data;
                    const balData: any = await global_helper.Fetch_Balance(coinData, data.wallet_address)
                    if (balData.status) {
                        const currentBalance = data.balance !== undefined ? Number(data.balance) : undefined;
                        if (currentBalance === undefined || currentBalance !== balData.balance) {
                            console.log("balance changed >>>>>>.", data.wallet_address)
                            await wallet_queries.wallet_update(
                                { balance: balData.balance, status: 1 },
                                { coin_id: coinData.coin_id, wallet_address: data.wallet_address }
                            )
                        } else {
                            //console.log("balance unchanged >>>>>>.", data.wallet_address, 'balance', balData.balance)
                        }
                    } else {
                        console.log("not got balance data of this wallet addres, reason node error")
                        if (data.retry_count <= 5) {
                            const retryCount = data.retry_count + 1;
                            await kafkaHelper.produceMessage(this.trx_pending_balance_update_topic, { wallet_address: data.wallet_address, coin_data: coinData, retry_count: retryCount })
                        }
                    }
                } else {
                    console.log("data not present for this coin id.", data.coin_data)
                }
            } else {
                console.log("No data present>>>")
            }
        } catch (err: any) {
            console.error("Error in userBalance>>>", err)
        }
    }

    public startUpdateAllBalances = async () => {
        let consumer = await kafkaHelper.kafka.consumer({ groupId: this.update_user_all_coins_balance_group_id });
        await consumer.subscribe({ topic: this.update_user_all_coins_balance_topic, fromBeginning: true });
        await consumer.run({
            eachMessage: async ({ topic, partition, message }: { topic: any, partition: any, message: any }) => {
                // console.log("🚀 ~ startTxStatusUpdateQueue topic >>>>", topic);
                // console.log("🚀 ~ startTxStatusUpdateQueue message.value >>>>", message.value.toString());
                await this.userAllBalances(JSON.parse(message.value.toString()))
            }
        }).catch((err: Error) => {
            console.log("🔥 ~ ~ startTxStatusUpdateQueue error", err);
        });
    };

    public async userAllBalances(data: any) {
        try {
            console.log("data in user_all_balances user_all_balances >>>>>>.", data)
            if (data) {

                const userWalletData: any = await wallet_queries.wallet_with_coins_joint(
                    ["wallet_address", "coin_id"],
                    { user_id: data.user_id, coin_family: data.coin_family },
                    ["coin_id", "coin_family", "token_type", "coin_symbol", "is_token", "token_address"],
                    [['wallet_id', 'ASC']]
                )


                if (userWalletData.length > 0) {
                    for (let i: number = 0; i < userWalletData.length; i++) {
                        if (userWalletData[i].coin_data) {
                            const balData: any = await global_helper.Fetch_Balance(userWalletData[i].coin_data, userWalletData[i].wallet_address);

                            if (balData.status) {
                                console.log("coin_id>>>>>", userWalletData[i].coin_data.coin_id, "wallet_address>>>", userWalletData[i].wallet_address, "balance>>>", balData.balance)

                                await wallet_queries.wallet_update(
                                    { balance: balData.balance },
                                    { coin_id: userWalletData[i].coin_data.coin_id, wallet_address: userWalletData[i].wallet_address }
                                )
                                console.log("updated succssffullylyyyy")
                            } else {
                                console.log("not got balance data of this wallet addres, reason node error under user_all_balances")
                                await kafkaHelper.produceMessage(this.trx_pending_balance_update_topic, { wallet_address: data.wallet_address, coin_data: userWalletData[i].coin_data, retry_count: 0 })
                            }
                        } else {
                            console.log("no coin data")
                        }
                    }

                } else {
                    console.log("Data not exist in all_coins>>")
                }
            } else {
                console.log("No data present>>>")
            }
        } catch (err: any) {
            console.error("Error in user_all_balances>>>", err)
        }
    }

    public updateWalletBalance = async () => {
        try {
            let coinLimitCount = 0;
            const PageCountKey = `WalletCount_${CoinFamily.TRON}`;
            const limit = 10;
            const findCoinCounter = await tronProcessHelper.getKeyValuePair(config.REDISKEYS.ALL_PROCESSES, PageCountKey)

            if (findCoinCounter) {
                coinLimitCount = Number(findCoinCounter);
            }

            const wallets: any = await WalletModel.findAll({
                attributes: ['wallet_address', 'balance'],
                where: {
                    coin_family: CoinFamily.TRON
                },
                include: [
                    {
                        model: CoinsModel,
                        as: 'coin_data',
                        required: true
                    }
                ],
                limit,
                offset: coinLimitCount,
                order: [['wallet_id', 'ASC']],
                nest: true,
            });
            if (!wallets) {
                await tronProcessHelper.setKeyValuePair(config.REDISKEYS.ALL_PROCESSES, PageCountKey, '0')
            }
            if (wallets.length > 0) {
                for (const wallet of wallets) {
                    const coin_data = wallet.coin_data;
                    if (!coin_data) return false;
                    const payload = {
                        coin_data: coin_data,
                        wallet_address: wallet.wallet_address,
                        balance: wallet.balance,
                        retry_count: 0,
                    };
                    await kafkaHelper.produceMessage(this.trx_pending_balance_update_topic, payload);
                }
            }
            if (wallets.length < limit) {
                await tronProcessHelper.setKeyValuePair(config.REDISKEYS.ALL_PROCESSES, PageCountKey, '0')
            } else {
                coinLimitCount += limit;
                await tronProcessHelper.setKeyValuePair(config.REDISKEYS.ALL_PROCESSES, PageCountKey, coinLimitCount.toString())
            }
            return true;
        } catch (err: any) {
            console.error("Error in updateWalletBalanceByWallet", err)
        }
    };



}

export const updateBalanceProcess = new UpdateBalanceController();
