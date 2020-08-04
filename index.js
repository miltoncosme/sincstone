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
  console.log(obj);
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
      console.log(res.data);
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
            let xml = buff.toString("ascii");
            console.log(new Date(), "=>NFCe: ", xmlNFCeToJson(xml));
            const idempresa = obj.id;
            const pool = new Pool(conn2(namedb));
            const NFe =
              xml.nfeProc && xml.nfeProc.NFe ? xml.nfeProc.NFe : xml.NFe;
            const Aut =
              xml.nfeProc && xml.nfeProc.protNFe ? xml.nfeProc.protNFe : nil;
            console.log("idempresa:", idempresa, "NFe:", NFe, "Aut:", Aut);
            gravaVenda(idempresa, NFe, Aut);
            async function gravaVenda(idempresa, NFe, Aut) {
              try {
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
                  NFe.infNFe.det[0].CFOP,
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
                /*
                if (param.cancelado === true) {
                  await pool.query(`delete from vendarec where idvenda=${id}`);
                } else {
                  for (let i = 0; i < param.rec.length; i++) {
                    const r = param.rec[i];
                    const qryRecValues = [
                      id,
                      r.idfinalizadora,
                      r.descricaofin,
                      r.data,
                      r.hora,
                      r.valor,
                      r.troco,
                      r.tef,
                      r.nsu,
                      r.rede,
                      r.msg,
                      r.nrparcelas,
                      r.origem,
                    ];
                    await pool.query(qryRecIns, qryRecValues);
                  }
                }
                for (let x = 0; x < param.mov.length; x++) {
                  const m = param.mov[x];
                  const qryMovValues = [
                    id,
                    m.data,
                    m.hora,
                    m.codproduto,
                    m.descricao,
                    m.unidade,
                    m.antes,
                    m.qtd,
                    m.depois,
                    m.compra,
                    m.venda,
                    m.comissao,
                    m.cancelado,
                    m.desconto,
                    m.item,
                    m.subtotal,
                    m.idgrupo,
                    m.cfop,
                    m.sticms,
                    m.stpiscofins,
                    m.stipi,
                    m.aliicms,
                    m.alipis,
                    m.alicofins,
                    m.aliipi,
                    m.vlricms,
                    m.vlripi,
                    m.vlrpis,
                    m.vlrcofins,
                    m.ncm,
                    m.cest,
                    m.idusuacan,
                  ];
                  await pool.query(qryMovIns, qryMovValues);
                }
                if (param.xml) {
                  const qryXMLValues = [
                    id,
                    param.xml.datanf,
                    param.xml.chave,
                    param.xml.status,
                    param.xml.motivo,
                    param.xml.nfxml,
                  ];
                  await pool.query(qryVenXML, qryXMLValues);
                }
                */
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
