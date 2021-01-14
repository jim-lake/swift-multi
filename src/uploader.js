const async = require('async');
const fs = require('fs');
const { join: pathJoin } = require('path');
const request = require('request');

const { fetchAuth } = require('./auth');
const { sendChunks } = require('./chunk_uploader');

const CHUNK_SIZE = 100 * 1024 * 1024;
const REQ_TIMEOUT = 10 * 60 * 1000;

exports.sendFile = sendFile;

function sendFile(params, done) {
  const {
    os_auth_url,
    os_password,
    os_username,
    os_tenant_name,
    source_path,
    container,
    object_path,
    ip_list,
    send_per_ip,
    delete_at,
  } = params;
  const errorLog = params.error_log || function () {};
  const consoleLog = params.console_log || function () {};

  let endpoint_url;
  let keystone_auth;
  let file_size;
  const chunk_list = [];
  async.series(
    [
      (done) => {
        const opts = {
          os_auth_url,
          os_password,
          os_username,
          os_tenant_name,
        };
        fetchAuth(opts, (err, token, service_map) => {
          if (err) {
            errorLog('fetchAuth: failed err:', err);
          } else {
            keystone_auth = token;
            if (service_map.swift && service_map.swift[0]) {
              endpoint_url = service_map.swift[0].publicURL;
            }
            if (!endpoint_url) {
              errorLog('no swift url');
              err = 'no_endpoint';
            }
          }
          done(err);
        });
      },
      (done) => {
        fs.stat(source_path, (err, stats) => {
          if (err) {
            errorLog('stat on file failed:', err);
          } else {
            file_size = stats.size;
            for (let pos = 0, i = 0; pos < file_size; i++) {
              const size_left = file_size - pos;
              const size = Math.min(size_left, CHUNK_SIZE);
              chunk_list.push({
                index: i,
                size,
                start: pos,
                end: pos + size - 1,
              });
              pos += size;
            }
            if (chunk_list.length === 1) {
              chunk_list[0].object_path = object_path;
            }

            consoleLog(
              'Uploading:',
              source_path,
              '=>',
              container + '/' + object_path,
              'size:',
              file_size,
              'chunks:',
              chunk_list.length
            );
          }
          done(err);
        });
      },
      (done) => {
        const opts = {
          keystone_auth,
          endpoint_url,
          source_path,
          container,
          chunk_list,
          ip_list,
          send_per_ip,
          delete_at,
          error_log: errorLog,
        };
        sendChunks(opts, (err) => {
          if (err) {
            errorLog('send_chunks failed, err:', err);
          }
          done(err);
        });
      },
      (done) => {
        if (chunk_list.length === 1) {
          done();
        } else {
          const slo_content = chunk_list.map((chunk) => {
            return {
              path: pathJoin(container, chunk.object_path),
              etag: chunk.etag,
              size_bytes: chunk.size,
            };
          });
          const req = {
            method: 'PUT',
            url: container + '/' + object_path,
            qs: {
              'multipart-manifest': 'put',
            },
            body: slo_content,
            json: true,
            headers: {},
          };
          if (delete_at) {
            req.headers['X-Delete-At'] = delete_at;
          }
          const opts = { req, endpoint_url, keystone_auth };
          _send(opts, (err, body) => {
            if (err) {
              errorLog('create_slo err:', err, body);
            }
            done(err);
          });
        }
      },
    ],
    (err) => {
      done(err);
    }
  );
}

function _send(opts, done) {
  const { req, keystone_auth, endpoint_url } = opts;
  if (!req.headers) {
    req.headers = {};
  }
  req.headers['X-Auth-Token'] = keystone_auth;
  req.url = endpoint_url + '/' + req.url;
  req.timeout = REQ_TIMEOUT;
  request(req, (err, response, body) => {
    const statusCode = response && response.statusCode;
    if (!err && (statusCode < 200 || statusCode > 299)) {
      err = statusCode;
    }
    done(err, body, response);
  });
}
