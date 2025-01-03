// import { CoinFamily } from "../enum";
// import * as Models from '../models/model/index';
// import tronProcessHelper from '../processes/processes.helper';
// import { Blockchain_Helper } from "./blockchain.helper";
// import wallet_queries from "./dbhelper/wallets";
// import { Utility_Helper } from "./utility.helper";
// import utxo from './utxo';

// class WalletHelper {

//    // public async Update_Balance(wallet_address: string, coin: any) {
//    //    try {
//    //       if (!wallet_address) {
//    //          console.error(`wallet_address missing >>> `);
//    //          return false;
//    //       }
//    //       /* fatch wallet balnce */
//    //       const balance: number = await this.Fetch_Balance(
//    //          wallet_address,
//    //          coin
//    //       );

//    //       let wallet_data: any = await wallet_queries.wallet_find_one(
//    //          ["balance"],
//    //          { wallet_address: wallet_address, coin_id: coin.coin_id }
//    //       )

//    //       if (wallet_data) {

//    //          await wallet_queries.wallet_update(
//    //             { balance: balance, status: 1 },
//    //             { wallet_address: wallet_address, coin_id: coin.coin_id, }
//    //          )

//    //       }
//    //       return balance;
//    //    } catch (err: any) {
//    //       console.error(`WalletHelper Update_Balance error >>>`, err);
//    //       return {};
//    //    }
//    // }
//    // public async Fetch_Balance(address: string, coin: any,) {
//    //    try {

//    //       let balance: number = 0;
//    //       if (coin) {
//    //          switch (coin.coin_family) {
//    //             case CoinFamily.BNB:
//    //                console.log("updating bnb address balance", address)
//    //                balance = await Blockchain_Helper.Fetch_Balance_bnb(address, coin);
//    //                break;
//    //             case CoinFamily.ETH:
//    //                console.log("updating eth address balance", address)
//    //                balance = await Blockchain_Helper.ETH_Fetch_Balance(address, coin);
//    //                break;
//    //             case CoinFamily.BTC:
//    //                console.log("updating btc address balance", address)
//    //                balance = Number(await utxo.getUserBtcBalance(address));
//    //                break;
//    //             case CoinFamily.MATIC:
//    //                console.log("updating matic address balance", address)
//    //                balance = await Blockchain_Helper.MATIC_Fetch_Balance(address, coin);
//    //                break;
//    //             case CoinFamily.TRON:
//    //                console.log("updating tron address balance", address)
//    //                balance = await Blockchain_Helper.TRX_Fetch_Balance(address, coin);
//    //                break;
//    //             default:
//    //                balance = 0;
//    //                break;
//    //          }
//    //       }
//    //       return Number(await Utility_Helper.exponentialToDecimal(balance));
//    //    } catch (error: any) {
//    //       console.error(`WalletHelper Fetch_Balance error >>>`, error);
//    //       return 0;
//    //    }
//    // }
//    // public async Update_all_active_coin_balance(wallet_address: string) {
//    //    try {
//    //       let address_coins: any = await Models.WalletModel.findAll({
//    //          attributes: ["coin_id", "wallet_address"],
//    //          where: {
//    //             wallet_address: wallet_address,
//    //             status: 1
//    //          },
//    //          include: [{
//    //             model: Models.CoinsModel,
//    //             attributes: ["coin_id", "coin_name", "coin_symbol", "coin_family", "is_token", "token_type", "token_address"],
//    //             as: "coin_data",
//    //             required: true,
//    //             where: {
//    //                coin_status: 1,
//    //             },
//    //          }],
//    //       })
//    //       for (let i = 0; i < address_coins.length; i++) {
//    //          await this.Update_Balance(address_coins[i].wallet_address, address_coins[i].coin_data);
//    //       }
//    //       return 1;

//    //    } catch (err: any) {
//    //       console.error(`WalletHelper Update_all_active_coin_balance error >>>`, err);
//    //       return 0;

//    //    }
//    // }

// }
// export let Wallet_Helper = new WalletHelper();
