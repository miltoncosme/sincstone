//process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';
require("dotenv/config");
const axios = require("axios").default;
const CronJob = require("cron").CronJob;
const { Pool } = require("pg");
const { conn } = require("../db.js");
const { conn2 } = require("../db2.js");

const qryBuscPDV = `select b.cnpj, a.caixa, a.serial from configcaixa a,empresa b
                    where a.idempresa=b.id and a.equipamento=3`;

let pool = new Pool({
  user: process.env.USER_DB,
  password: process.env.PASS_DB,
  host: "localhost",
  database: "postgres",
  port: process.env.PORT_DB,
});
pool
  .query(
    `SELECT datname FROM pg_database WHERE datistemplate = false and datname like 'DB-SL-%'`
  )
  .then((con) => {
    con.rows.map((db) => {
      let pool = new Pool(conn2(db.datname));
      pool
        .query(qryBuscPDV)
        .then((con) => {
          con.rows.map((pdv) => {
            const job = new CronJob(
              "*/25 * * * * *",
              function () {
                sincVendas(pdv);
              },
              null,
              false,
              "America/Sao_Paulo"
            );
            job.start();
          });
        })
        .catch((err) => {
          console.log("erro: ", err.message);
        });
    });
  })
  .catch((err) => console.log(err.message));

function sincVendas(obj) {
  console.log(obj);
  const urlAutNFCe = `${process.env.URL_MAMBA}/api/v1/stone/pegaXML/`;
  const urlAutNFCeTratados = `${process.env.URL_MAMBA}/api/v1/stone/pegaXMLTratados/`;

  axios({
    method: "get",
    url: `${urlAutNFCe}${obj.serial}`,
    headers: { "Content-Type": "application/json" },
    auth: {
      username: process.env.USER_MAMBA,
      password: process.env.PASS_MAMBA,
    },
  })
    .then((con) => {
      console.log(new Date(), "=>NFCe: ", con.data.dados);
    })
    .catch((err) => {
      console.log(err.message);
    });

  axios({
    method: "get",
    url: `${urlAutNFCeTratados}${obj.serial}`,
    headers: { "Content-Type": "application/json" },
    auth: {
      username: process.env.USER_MAMBA,
      password: process.env.PASS_MAMBA,
    },
  })
    .then((con) => {
      console.log(new Date(), "=>NFCeTratados: ", con.data.dados);
    })
    .catch((err) => {
      console.log(err.message);
    });
}
