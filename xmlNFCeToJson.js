let xmlParser = require("xml2json");

function xmlNFCeToJson(xml) {
  return xmlParser.toJson(xml, { object: true });
}

module.exports = xmlNFCeToJson;
