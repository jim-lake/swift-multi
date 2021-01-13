#!/usr/bin/env node
'use strict';

const { dirname } = require('path');
const { argv } = require('yargs');
const SwiftMulti = require('./index');
const { env } = process;

const SEND_PER_IP_DEFAULT = 3;

const command_path = process.argv[1];

function usage() {
  const name = command_path.slice(dirname(command_path).length + 1);
  console.error(
    `Usage: ${name} <upload|download> <container> <source_path>
  [--object_path object_path]
  [--os_auth_url url]
  [--os_password password]
  [--os_tenant_name tenant]
  [--os_username username]
  [--send_per_ip count]
  [--ip_list list]
  [--delete_at unixtime]
  [--delete_after seconds]
`
  );
}

const method = argv._[0];
const container = argv._[1];
const source_path = argv._[2];
const os_auth_url = argv.os_auth_url || env.OS_AUTH_URL;
const os_password = argv.os_password || env.OS_PASSWORD;
const os_tenant_name = argv.os_tenant_name || env.OS_TENANT_NAME;
const os_username = argv.os_username || env.OS_USERNAME;
const send_per_ip = argv.send_per_ip || SEND_PER_IP_DEFAULT;
const ip_list = (argv.ip_list || '').split(',');
const object_path = argv.object_path || source_path;
const delete_after = parseInt(argv.delete_after);

let delete_at = parseInt(argv.delete_at || '0');
if (!delete_at && delete_after) {
  delete_at = Math.floor(Date.now() / 1000) + delete_after;
}

if (argv.help) {
  usage();
  process.exit(-1);
}
if (!os_auth_url || !os_password || !os_tenant_name || !os_username) {
  console.error('No auth found.');
  process.exit(-254);
}
if (method !== 'upload' && method !== 'download') {
  console.error('Invalid method, provide upload or download.');
  process.exit(-253);
}
if (!source_path) {
  usage();
  process.exit(-1);
}
if (!container) {
  usage();
  process.exit(-1);
}

if (ip_list.length === 1 && !ip_list[0].length) {
  ip_list.splice(0, 1);
}
if (ip_list && ip_list.length > 1) {
  _consoleLog('Source IPs:', ip_list);
}
if (delete_at) {
  _consoleLog('Delete at:', delete_at);
}

if (method === 'upload') {
  const opts = {
    os_auth_url,
    os_username,
    os_password,
    os_tenant_name,
    source_path,
    container,
    object_path,
    ip_list,
    send_per_ip,
    delete_at,
    error_log: _errorLog,
    console_log: _consoleLog,
  };
  SwiftMulti.sendFile(opts, (err) => {
    if (err) {
      console.error('failed:', err);
      process.exit(-1);
    } else {
      console.log('success!');
      process.exit(0);
    }
  });
} else if (method === 'download') {
  console.error('not implemented');
  process.exit(-1);
}

function _errorLog(...args) {
  console.error(...args);
}
function _consoleLog(...args) {
  console.log(...args);
}
