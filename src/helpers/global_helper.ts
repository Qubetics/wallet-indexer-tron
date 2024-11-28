import axios from 'axios';
import WalletModel from '../models/model/model.wallets'
import tronProcessHelper from '../processes/processes.helper';
import { config } from '../config';
import { CatchErrorMsgsModel } from '../models/model';
import { CoinFamily } from '../enum';
import { Blockchain_Helper } from './blockchain.helper';
import utxo from './utxo';


class Global_helper {
    public async save_error_logs(fn_name: string, error_message: any) {
        try {
            let value_exist: any = await tronProcessHelper.getKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.TRON_ERROR_LOGS)
            if (value_exist) {
                if (Number(value_exist) == 0) {
                    await CatchErrorMsgsModel.create({
                        fx_name: fn_name,
                        error_msg: error_message || {}
                    })
                    await tronProcessHelper.setKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.TRON_ERROR_LOGS, '1')
                } else {
                    console.log("value other than 0 exist")
                }
            } else {
                await tronProcessHelper.setKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.TRON_ERROR_LOGS, '0')
                console.log("value does not exist")
            }
        } catch (err: any) {
            console.error("Error in save_error_logs", err)
        }
    }
    public async Fetch_Balance(coin_data: any, wallet_address: string,) {
        let bal_data: { status: boolean, balance: number } = { status: false, balance: 0 };
        try {

            switch (coin_data.coin_family) {
                // case CoinFamily.BNB:
                //     console.log("bnb bnb")
                //     let bsc_data: any = await Blockchain_Helper.Fetch_Balance_bnb(wallet_address, coin_data)
                //     if (bsc_data.status) {
                //         return { status: true, balance: bsc_data.balance }
                //     }
                //     break;

                // case CoinFamily.ETH:
                //     console.log("eth eth")
                //     let eth_data: any = await Blockchain_Helper.ETH_Fetch_Balance(wallet_address, coin_data)
                //     if (eth_data.status) {
                //         return { status: true, balance: eth_data.balance }
                //     }
                //     break;

                // case CoinFamily.BTC:
                //     console.log("btc btc")
                //     let btc_data: any = await utxo.getUserBtcBalance(wallet_address)
                //     if (btc_data.status) {
                //         return { status: true, balance: btc_data.balance }
                //     }
                //     break;

                case CoinFamily.TRON:
                    console.log("trx trx")
                    let tron_data: any = await Blockchain_Helper.TRX_Fetch_Balance(wallet_address, coin_data)
                    if (tron_data.status) {
                        return { status: true, balance: tron_data.balance }
                    }
                    break;
            }
            return bal_data;
        } catch (err: any) {
            console.error(`error in WalletHelper Fetch_Balance error >>>`, err);
            return bal_data;
        }
    }
    public async check_coin_in_wallet(wallet_address: string, coin: any, balance: any) {
        try {
            let wallet_data: any = await WalletModel.findOne({
                attributes: ["balance"],
                where: {
                    wallet_address: wallet_address,
                    coin_id: coin.coin_id,
                }
            })
            if (!wallet_data) {
                let wallet_data: any = await tronProcessHelper.check_our_wallet_address(wallet_address)
                let data_obj: any = {
                    user_id: wallet_data.user_id,
                    email: wallet_data.email,
                    wallet_name: wallet_data.wallet_name,
                    wallet_address: wallet_address,
                    coin_id: coin.coin_id,
                    coin_family: coin.coin_family,
                    balance: balance,
                    default_wallet: 1,
                    status: 1
                }
                await WalletModel.create(data_obj)
            }
        } catch (err: any) {
            console.error("Error in check_coin_in_wallet>>>", err)
            return err;
        }

    }
    public async send_mail(email: string, text: string, subject: string) {
        try {
            const MAILGUN_API_KEY: string = `${config.MAILGUN.MAILGUN_API_KEY}`;
            const MAILGUN_DOMAIN: string = `${config.MAILGUN.MAILGUN_DOMAIN}`;
            const MAILGUN_BASE_URL: string = `${config.MAILGUN.MAILGUN_BASE_URL}${MAILGUN_DOMAIN}`;
            const response: any = await axios({
                method: 'post',
                url: `${MAILGUN_BASE_URL}/messages`,
                auth: {
                    username: 'api',
                    password: MAILGUN_API_KEY
                },
                params: {
                    from: config.MAILGUN.FROM_GMAIL,
                    to: email,
                    subject: subject,
                    html: text
                }
            });
        } catch (err: any) {
            console.error("send_mail", err)
            await this.save_error_logs("TRON_MAILGUN_ERROR", err.message)
        }
    }
}
export let global_helper = new Global_helper();
