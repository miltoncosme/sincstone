require("dotenv/config");
const xmlNFCeToJson = require("./xmlNFCeToJson");

const axios = require("axios").default;
const CronJob = require("cron").CronJob;
const { Pool } = require("pg");
const { conn2 } = require("../db2.js");

const qryBuscPDV = `select b.id, b.cnpj, a.caixa, a.serial from configcaixa a,empresa b
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
                sincVendas(pdv, db.datname);
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

function sincVendas(obj, namedb) {
  const url = `${process.env.URL_MAMBA}/api/v1/stone/empresa/${obj.cnpj}`;
  axios({
    method: "get",
    url: url,
    headers: { "Content-Type": "application/json" },
    auth: {
      username: process.env.USER_MAMBA,
      password: process.env.PASS_MAMBA,
    },
  })
    .then((res) => {
      const urlAutNFCe = `${process.env.URL_MAMBA}/api/v1/stone/pegaXML/`;
      const urlAutNFCeTratados = `${process.env.URL_MAMBA}/api/v1/stone/pegaXMLTratados/`;
      axios({
        method: "get",
        url: `${urlAutNFCe}${res.data.empresa.id}`,
        headers: { "Content-Type": "application/json" },
        auth: {
          username: process.env.USER_MAMBA,
          password: process.env.PASS_MAMBA,
        },
      })
        .then((con) => {
          const { dados } = con.data;
          dados.map((venda) => {
            const nfceBase64 = venda.nfce;
            let buff = new Buffer(nfceBase64, "base64");
            let XML = buff.toString("ascii");
            const xml = xmlNFCeToJson(XML);
            const idempresa = obj.id;
            const pool = new Pool(conn2(namedb));
            if (xml.nfeProc && xml.nfeProc.NFe) {
              const { NFe } = xml.nfeProc;
              gravaVenda(idempresa, NFe);
            } else if (xml.NFe) {
              const { NFe } = xml;
              gravaVenda(idempresa, NFe);
            }

            async function gravaVenda(idempresa, NFe, Aut) {
              try {
                console.log(NFe.infNFe.det);

                const qryValues = [
                  idempresa,
                  NFe.infNFe.ide.dhEmi,
                  String(NFe.infNFe.infAdic.infCpl)
                    .substring(
                      String(NFe.infNFe.infAdic.infCpl).indexOf(
                        "Documento Interno:"
                      ) + 18,
                      String(NFe.infNFe.infAdic.infCpl).indexOf(
                        "Documento Interno:"
                      ) + 25
                    )
                    .trim(),
                  ("000" + Number(NFe.infNFe.ide.serie)).slice(-3),
                  0,
                  null,
                  null,
                  2,
                  NFe.infNFe.det.prod.CFOP || NFe.infNFe.det[0].prod.CFOP,
                  false,
                  false,
                  NFe.infNFe.total.ICMSTot.vNF,
                  NFe.infNFe.total.ICMSTot.vICMS,
                  Number(NFe.infNFe.total.ICMSTot.vPIS) +
                    Number(NFe.infNFe.total.ICMSTot.vCOFINS),
                  0,
                  NFe.infNFe.total.ICMSTot.vDesc,
                  0,
                  NFe.infNFe.total.ICMSTot.vProd,
                  0,
                  NFe.infNFe.total.ICMSTot.vBC,
                  0,
                  null,
                  "65",
                  NFe.infNFe.ide.nNF,
                  null,
                  null,
                  null,
                ];
                await pool.query("BEGIN");
                const venda = await pool.query(qryVenIns, qryValues);
                const { id } = venda.rows[0];
                console.log("id de insert", id);

                await pool.query("COMMIT");
                console.log("OK");
              } catch (error) {
                await pool.query("ROLLBACK");
                const e = error.message;
                console.log(e);
                if (e.includes("duplicate")) {
                  //
                } else {
                  //
                }
              }
            }
          });
        })
        .catch((err) => {
          console.log(err.message);
        });
    })
    .catch((err) => {
      console.log(err.message);
    });
}

const qryVenIns = `insert into venda (
  idempresa,
  data,  
  documento,
  caixa,
  idusuarioope,
  idusuariores,
  idusuarioven,
  tipomovimento,
  cfop,
  cancelado,
  canrejeicao,
  valor,
  estadual,
  federal,
  ipi,
  desconto,
  acrescimo,
  subtotal,
  comissao,
  basecalcicms,
  icmssubst,
  pedido,
  tipodoc,
  numccf,
  cpfcnpjavulso,
  nomecliavulso,
  idcliente
) values (
  $1, to_timestamp($2,'DD/MM/YYYY HH24:MI:SS'), $3, $4, $5, $6, $7, $8, $9,$10,
$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,
$21,$22,$23,$24,$25,$26,$27)
on conflict (idempresa, documento, caixa, numccf) do update set
cancelado=$10  returning id`;
