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
      sinc(`${process.env.URL_MAMBA}/api/v1/stone/pegaXML/`);
      sinc(`${process.env.URL_MAMBA}/api/v1/stone/pegaXMLTratados/`);
      function sinc(urlAutNFCe) {
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
            dados.map((dadosVenda) => {
              const nfceBase64 = dadosVenda.nfce;
              let buff = new Buffer(nfceBase64, "base64");
              const XML = buff.toString("ascii");
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
                  const qryValues = [
                    idempresa,
                    NFe.infNFe.ide.dhEmi,
                    ("000000" + Number(NFe.infNFe.ide.nNF)).slice(-6),
                    ("000" + Number(NFe.infNFe.ide.serie)).slice(-3),
                    0,
                    null,
                    null,
                    2,
                    NFe.infNFe.det.prod
                      ? NFe.infNFe.det.prod.CFOP
                      : NFe.infNFe.det[0].prod.CFOP,
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
                    0,
                    "65",
                    NFe.infNFe.ide.nNF,
                    null,
                    null,
                    null,
                  ];
                  await pool.query("BEGIN");
                  const venda = await pool.query(qryVenIns, qryValues);
                  const { id } = venda.rows[0];
                  const itens = NFe.infNFe.det;
                  if (itens.length > 0) {
                    for (let x = 0; x < itens.length; x++) {
                      const m = itens[x];
                      const qryMovIns = `insert into vendamov (
                          idvenda, data, hora, codproduto, descricao, unidade,
                          antes, qtd, depois, compra, venda, comissao,
                          cancelado, desconto, item, subtotal, idgrupo,
                          cfop, sticms, stpiscofins, stipi, aliicms,
                          alipis, alicofins, aliipi, vlricms, vlripi,
                          vlrpis, vlrcofins, ncm, cest, idusuacan
                        ) 
                        select ${id}, to_date('${String(
                        NFe.infNFe.ide.dhEmi
                      ).substring(0, 10)}', 'yyyy-mm-dd'), '${String(
                        NFe.infNFe.ide.dhEmi
                      ).substring(11, 19)}',
                          a.codproduto, a.descricao, a.unidade, 0, ${Number(
                            m.prod.qCom
                          )}, 0, a.compra, ${Number(m.prod.vUnCom)},
                          0, false, 0, '${m.nItem}', ${Number(
                        m.prod.vProd
                      )}, a.codgrupo, b.cfopvenda, b.csticms, b.cstpiscofins, 0, b.aliquotaicms,b.aliquotapis,
                  b.aliquotacofins,0,
                          (b.aliquotaicms*${Number(
                            m.prod.vProd
                          )})*0.01,0,(b.aliquotapis*${Number(
                        m.prod.vProd
                      )})*0.01,
                          (b.aliquotacofins*${Number(
                            m.prod.vProd
                          )})*0.01, b.ncm, b.cest, null from produto a
                           left join produtoimp b on(a.id=b.idproduto) where a.codproduto='${
                             m.prod.cProd
                           }' 
                        on conflict (idvenda, item) do update set
                        cancelado=false returning id`;
                      await pool.query(qryMovIns);
                    }
                  } else {
                    const m = itens;
                    const qryMovIns = `insert into vendamov (
                          idvenda, data, hora, codproduto, descricao, unidade,
                          antes, qtd, depois, compra, venda, comissao,
                          cancelado, desconto, item, subtotal, idgrupo,
                          cfop, sticms, stpiscofins, stipi, aliicms,
                          alipis, alicofins, aliipi, vlricms, vlripi,
                          vlrpis, vlrcofins, ncm, cest, idusuacan
                        ) 
                        select ${id}, to_date('${String(
                      NFe.infNFe.ide.dhEmi
                    ).substring(0, 10)}', 'yyyy-mm-dd'), '${String(
                      NFe.infNFe.ide.dhEmi
                    ).substring(11, 19)}',
                          a.codproduto, a.descricao, a.unidade, 0, ${Number(
                            m.prod.qCom
                          )}, 0, a.compra, ${Number(m.prod.vUnCom)},
                          0, false, 0, 1, ${Number(
                            m.prod.vProd
                          )}, a.codgrupo, b.cfopvenda, b.csticms, b.cstpiscofins, 0, b.aliquotaicms,b.aliquotapis,
                  b.aliquotacofins,0,
                          (b.aliquotaicms*${Number(
                            m.prod.vProd
                          )})*0.01,0,(b.aliquotapis*${Number(
                      m.prod.vProd
                    )})*0.01,
                          (b.aliquotacofins*${Number(
                            m.prod.vProd
                          )})*0.01, b.ncm, b.cest, null from produto a
                           left join produtoimp b on(a.id=b.idproduto) where a.codproduto='${
                             m.prod.cProd
                           }' 
                        on conflict (idvenda, item) do update set
                        cancelado=false returning id`;
                    await pool.query(qryMovIns);
                  }

                  const { pag } = NFe.infNFe;
                  if (pag.length > 0) {
                    for (let i = 0; i < pag.length; i++) {
                      const r = pag[i];
                      const qryRecIns = `insert into vendarec (
                      idvenda,
                      idfinalizadora,
                      descricaofin,
                      data,
                      hora,  
                      valor,
                      troco,
                      tef,
                      nsu,
                      rede,
                      msg,
                      nrparcelas,
                      origem,
                      ntef
                    ) values (
                      ${id},
                      ${r.detPag.tPag == "01" ? 1 : 2},
                      '${r.detPag.tPag == "01" ? "Dinheiro" : "POS"}',
                      to_date('${String(NFe.infNFe.ide.dhEmi).substring(
                        0,
                        10
                      )}', 'yyyy-mm-dd'), 
                      '${String(NFe.infNFe.ide.dhEmi).substring(11, 19)}',
                      ${r.detPag.vPag},
                      0,
                      0,
                      'NA',
                      'NA',
                      '',
                      1,
                      0,
                      ${i + 1}) 
                      on conflict (idvenda, ntef) do update set
                      valor=${r.detPag.vPag}`;

                      await pool.query(qryRecIns);
                    }
                  } else {
                    const qryRecIns = `insert into vendarec (
                      idvenda,
                      idfinalizadora,
                      descricaofin,
                      data,
                      hora,  
                      valor,
                      troco,
                      tef,
                      nsu,
                      rede,
                      msg,
                      nrparcelas,
                      origem,
                      ntef
                    ) values (
                      ${id},
                      ${pag.detPag.tPag == "01" ? 1 : 2},
                      '${pag.detPag.tPag == "01" ? "Dinheiro" : "POS"}',
                      to_date('${String(NFe.infNFe.ide.dhEmi).substring(
                        0,
                        10
                      )}', 'yyyy-mm-dd'), 
                      '${String(NFe.infNFe.ide.dhEmi).substring(11, 19)}',
                      ${pag.detPag.vPag},
                      0,
                      0,
                      'NA',
                      'NA',
                      '',
                      1,
                      0,
                      1)
                      on conflict (idvenda, ntef) do update set
                      valor=${pag.detPag.vPag}`;
                    await pool.query(qryRecIns);
                  }

                  const qryXMLValues = [
                    id,
                    NFe.infNFe.ide.dhEmi,
                    NFe.infNFe.Id.substring(3, 47),
                    xml.nfeProc.protNFe.infProt.cStat || "9",
                    xml.nfeProc.protNFe.infProt.xMotivo ||
                      "Emissão em Contingência",
                    nfceBase64,
                  ];
                  await pool.query(qryVenXML, qryXMLValues);
                  console.log("id de insert", id);
                  axios({
                    method: "post",
                    url: `${urlAutNFCe}${dadosVenda.id}`,
                    headers: { "Content-Type": "application/json" },
                    auth: {
                      username: process.env.USER_MAMBA,
                      password: process.env.PASS_MAMBA,
                    },
                  })
                    .then(() => {
                      pool.query("COMMIT");
                      console.log("OK");
                    })
                    .catch((err) => {
                      pool.query("ROLLBACK");
                      console.log(err.message);
                    });
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
      }
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
  $1, $2, $3, $4, $5, $6, $7, $8, $9,$10,
$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,
$21,$22,$23,$24,$25,$26,$27)
on conflict (idempresa, documento, caixa, numccf) do update set
cancelado=$10  returning id`;

const qryVenXML = `insert into vendaxml(
                    idvenda, 
                    datanf, 
                    chave, 
                    status, 
                    motivo, 
                    nfxml
                  )values(
                    $1,$2,$3,$4,$5,$6)  
                  on conflict (idvenda) do update set
                  datanf=$2,
                  chave=$3,
                  status=$4,
                  motivo=$5,
                  nfxml=$6 returning id`;

module.exports = sincVendas;
