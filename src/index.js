const Downloader = require('./downloader');
const Uploader = require('./uploader');

exports.sendFile = Uploader.sendFile;
exports.downloadFile = Downloader.downloadFile;
