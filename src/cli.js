#!/usr/bin/env node
'use strict';

const yargs = require('yargs');
const SwiftMulti = require('./index');
const { env } = process;

const SEND_PER_IP_DEFAULT = 3;
const MB = 1024 * 1024;

const USAGE = `Usage: $0 <upload|download> <container> <local_path>
  [--object_path object_path]
  [--os_auth_url url]
  [--os_tenant_name tenant]
  [--os_username username]
  [--os_password password]
  [--send_per_ip count]
  [--ip_list list]
  [--delete_at unixtime]
  [--delete_after seconds]
  [--resume boolean]
  [--overwrite boolean]
  [--retry_count count]
  [--sha256 file_hash]
`;

const argv = yargs.usage(USAGE).argv;

const method = argv._[0];
const container = argv._[1];
const local_path = argv._[2];
const os_auth_url = argv.os_auth_url || env.OS_AUTH_URL;
const os_password = argv.os_password || env.OS_PASSWORD;
const os_tenant_name = argv.os_tenant_name || env.OS_TENANT_NAME;
const os_username = argv.os_username || env.OS_USERNAME;
const send_per_ip = argv.send_per_ip || SEND_PER_IP_DEFAULT;
const ip_list = (argv.ip_list || '').split(',');
const object_path = argv.object_path || local_path;
const delete_after = parseInt(argv.delete_after);
const resume = argv.resume || false;
const overwrite = argv.overwrite || false;
const retry_count = parseInt(argv.retry_count) || 1;
const sha256_hash = String(argv.sha256 || '');

let delete_at = parseInt(argv.delete_at || '0');
if (!delete_at && delete_after) {
  delete_at = Math.floor(Date.now() / 1000) + delete_after;
}

if (argv.help) {
  yargs.showHelp('log');
  process.exit(-1);
}
if (!os_auth_url || !os_password || !os_tenant_name || !os_username) {
  console.error('No auth found.');
  process.exit(-254);
}
if (!method) {
  yargs.showHelp('log');
  process.exit(-253);
}
if (method !== 'upload' && method !== 'download') {
  console.error('Invalid method, provide upload or download.');
  process.exit(-253);
}
if (!local_path) {
  yargs.showHelp('log');
  process.exit(-1);
}
if (!container) {
  yargs.showHelp('log');
  process.exit(-1);
}

if (ip_list.length === 1 && !ip_list[0].length) {
  ip_list.splice(0, 1);
}
if (ip_list && ip_list.length > 1) {
  console.log('Source IPs:', ip_list);
}
if (delete_at) {
  console.log('Delete at:', delete_at);
}

const start_time = Date.now();

if (method === 'upload') {
  const opts = {
    os_auth_url,
    os_username,
    os_password,
    os_tenant_name,
    source_path: local_path,
    container,
    object_path,
    ip_list,
    send_per_ip,
    delete_at,
    sha256_hash,
    error_log: _errorLog,
    console_log: _consoleLog,
  };
  SwiftMulti.sendFile(opts, (err, byte_count) => {
    if (err) {
      console.error('failed:', err);
      process.exit(-1);
    } else {
      const delta_s = (Date.now() - start_time) / 1000;
      const mbps = ((byte_count * 8) / MB / delta_s).toFixed(3);
      console.log(
        'uploaded:',
        local_path,
        'time:',
        delta_s + 's',
        'speed:',
        mbps + 'mbps'
      );
      process.exit(0);
    }
  });
} else if (method === 'download') {
  const opts = {
    os_auth_url,
    os_username,
    os_password,
    os_tenant_name,
    dest_path: local_path,
    container,
    object_path,
    resume,
    overwrite,
    retry_count,
    error_log: _errorLog,
    console_log: _consoleLog,
  };
  SwiftMulti.downloadFile(opts, (err, byte_count) => {
    if (err) {
      console.error('failed:', err);
      process.exit(-1);
    } else {
      const delta_s = (Date.now() - start_time) / 1000;
      const mbps = ((byte_count * 8) / MB / delta_s).toFixed(3);
      console.log(
        'downloaded:',
        local_path,
        'time:',
        delta_s + 's',
        'speed:',
        mbps + 'mbps'
      );
      process.exit(0);
    }
  });
}

function _errorLog(...args) {
  console.error(...args);
}
function _consoleLog(...args) {
  console.log(...args);
}
