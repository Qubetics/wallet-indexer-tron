import TronWeb from 'tronweb';
import { TokenStandard } from "../enum";
import { Utility_Helper } from "./utility.helper";
import { config } from '../config/config';
import tronProcessHelper from '../processes/processes.helper';
import node_issue_error_log_queries from './dbhelper/node_issue_error_logs';
class BlockchainHelper {

   public tron_coin_family: any = config.STATIC_COIN_FAMILY.TRX;
   public TRX_FULLNODE: string = config.NODE.TRX_RPC_URL;
   public tronWeb: any;
   constructor() {
      /* TRON CASE */
      this.tronWeb = new TronWeb({
         fullHost: this.TRX_FULLNODE,
         headers: { apikey: config.NODE.TRX_API_KEY }
      });


   }

   public async GetConfirmedTransaction(tx_hash: string, block_number: number) {
      try {
         const transaction: {
            status: any;
            data: { ret: [{ contractRet: any }] };
         } = await this.tronWeb.trx.getConfirmedTransaction(tx_hash).then((result: {}) => {
            return { status: true, data: result };
         }).catch(async (err: any) => {
            console.error("Error in getConfirmedTransaction catch:", err?.message);
            if (err == "Transaction not found") {
               console.log("Transaction not fount for tx_id>>", tx_hash, "block number>>>", block_number)
            } else {
               console.log("Error in GetConfirmedTransaction tx_id", tx_hash)
               await node_issue_error_log_queries.node_issue_error_logs_create({
                  function: "GetConfirmedTransaction",
                  block_number: block_number.toString(),
                  error: err.message,
                  transaction_id: tx_hash,
                  from_adrs: null,
                  to_adrs: null,
                  coin_family: this.tron_coin_family,
                  extra: "else catch under GetConfirmedTransaction"
               })
            }
            return { status: false, data: err };
         });
         return transaction;
      } catch (err: any) {
         console.error(`Error in GetConfirmedTransaction function >>`, err);
         await node_issue_error_log_queries.node_issue_error_logs_create({
            function: "GetConfirmedTransaction",
            block_number: block_number.toString(),
            error: err.message,
            transaction_id: tx_hash,
            from_adrs: null,
            to_adrs: null,
            coin_family: this.tron_coin_family,
            extra: "catch under GetConfirmedTransaction"
         })
         return { status: false, data: err };
      }
   }

   /* TRON CASE */
   public async Trc20_Token(address: string) {
      return await tronProcessHelper.checkIfContract(address)
   };
   /** fetch balance */
   public async TRX_Fetch_Balance(address: string, coin: any) {
      let bal_data: { status: boolean, balance: number } = { status: false, balance: 0 };
      try {
         if (coin.is_token == 1) {

            switch (coin.token_type.toLowerCase()) {

               case TokenStandard.TRC20:

                  let token_data: any = await this.TRC20_Token_Balance(
                     address, //// wallet_address
                     coin.token_address /// contract_address
                  )
                  if (token_data.status) {
                     return { status: true, balance: token_data.balance }
                  }
                  break;
            }
         } else {
            let coin_data: any = await this.TRX_Coin_Fetch_Balance(address);
            if (coin_data.status) {
               return { status: true, balance: coin_data.balance }
            }
         }
         return bal_data;
      } catch (error: any) {
         console.error(`error in TRON_Helper TRON_Fetch_Balance error >>`, error);
         return bal_data;

      }
   }
   public async TRC20_Token_Balance(address: string, contract_address: string) {
      let bal_data: { status: boolean, balance: number } = { status: false, balance: 0 };
      try {
         await this.tronWeb.setAddress(contract_address);
         let contract: any = await this.tronWeb.contract(config.TRON_ABI, contract_address);

         let decimals = await contract.decimals().call();
         if (decimals._hex != undefined) {
            decimals = await this.tronWeb.toDecimal(decimals._hex);
         }
         let balanceOf = await contract.balanceOf(address).call();
         let toDecimal = await this.tronWeb.toDecimal(balanceOf._hex);
         if (balanceOf._hex == undefined) {
            toDecimal = await this.tronWeb.toDecimal(
               balanceOf.balance._hex
            );
         }
         let expToDecimal = await Utility_Helper.exponentialToDecimal(toDecimal);
         let balance = Number(expToDecimal) / Math.pow(10, decimals);
         return { status: true, balance: Number(balance) };

      } catch (error: any) {
         console.error(`error in Tron_Helper TRC20_Token_Balance error >>`, error);
         return bal_data;
      }
   }
   /** fetch native coin balance */
   public async TRX_Coin_Fetch_Balance(address: string) {
      let bal_data: { status: boolean, balance: number } = { status: false, balance: 0 };
      try {
         return await this.tronWeb.trx.getBalance(address).then(async (result: any) => {
            let coinBalance: any = await this.tronWeb.fromSun(result);
            return { status: true, balance: Number(coinBalance) }
         });
      } catch (error: any) {
         console.error(`error in Tron_Helper TRX_Fetch_Balance error >>`, error);
         return bal_data;
      }
   }
   public async GetTransactionInfo(tx_hash: string) {
      try {
         return await this.tronWeb.trx.getTransactionInfo(tx_hash).then(async (result: any) => {
            return { status: true, data: result };
         });
      } catch (error: any) {
         console.error(`Tron_Helper GetTransactionInfo error >>`, error);
         return null;
      }
   }


}
export let Blockchain_Helper = new BlockchainHelper();