import { DBInterface } from "../interface/index";
import { Sequelize } from "sequelize";
import { config } from "../config/config";
import "dotenv/config";

class Database implements DBInterface {
   public db_write: Sequelize;
   public db_read: Sequelize;
   public connectionWrite: string;
   public connectionRead: string;

   constructor() {
      this.connectionWrite = `mysql://${config.DB_USER}:${config.DB_PASSWORD}@${config.DB_HOST_WRITE}/${config.DB_NAME}`;
      this.connectionRead = `mysql://${config.DB_USER}:${config.DB_PASSWORD}@${config.DB_HOST_READ}/${config.DB_NAME}`;


      this.db_write = new Sequelize(this.connectionWrite, {
         dialect: "mysql",
         logging: false,
         define: {
            charset: "utf8",
            collate: "utf8_general_ci",
            underscored: true,
            timestamps: true,
         },
         pool: {
            max: 5,
            min: 0,
            idle: 20000,
            acquire: 60000,
         },
         // pool: {
         //    max: 50,          // or 20–100 based on expected traffic
         //    min: 10,          // keep some idle connections ready
         //    idle: 30000,      // 30s idle timeout
         //    acquire: 60000,   // wait up to 60s for a free connection
         //    evict: 10000,     // optional: check and close idle every 10s
         // }
      });

      this.db_read = new Sequelize(this.connectionRead, {
         dialect: "mysql",
         logging: false,
         define: {
            charset: "utf8",
            collate: "utf8_general_ci",
            underscored: true,
            timestamps: true,
         },
         pool: {
            max: 5,
            min: 0,
            idle: 20000,
            acquire: 60000,
         },
      });
   }

}
const db = new Database();
export default {
   db_write: db.db_write,
   db_read: db.db_read,
};
