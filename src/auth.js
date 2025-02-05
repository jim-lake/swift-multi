const request = require('request');

const REQ_TIMEOUT = 10 * 1000;

exports.fetchAuth = fetchAuth;

function fetchAuth(params, done) {
  const { os_auth_url, os_password, os_username, os_project_name } = params;
  const opts = {
    url: os_auth_url + '/auth/tokens',
    method: 'POST',
    body: {
      auth: {
        scope: {
          project: { domain: { id: 'default' }, name: os_project_name },
        },
        identity: {
          password: {
            user: {
              domain: { id: 'default' },
              password: os_password,
              name: os_username,
            },
          },
          methods: ['password'],
        },
      },
    },
    json: true,
    timeout: REQ_TIMEOUT,
  };
  request(opts, (err, response, body) => {
    const statusCode = response && response.statusCode;
    if (!err && (statusCode < 200 || statusCode > 299)) {
      err = statusCode;
    }
    let token_id;
    const service_map = {};
    if (!err && body && response) {
      token_id = response.headers && response.headers['x-subject-token'];
      const catalog = body.token && body.token.catalog;
      if (catalog && catalog.length > 0) {
        catalog.forEach((service) => {
          const { name, endpoints } = service;
          service_map[name] = endpoints;
        });
      }
    }
    done(err, token_id, service_map);
  });
}
