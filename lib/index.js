/**
 * dbagent - client for Sqlite3
 */
const util = require('util');
const schedule = require('node-schedule');
const client = require('./client');
const utils = require('./utils');

module.exports = async function(channel, opt, logger) {
  this.logger = logger;
  const options = getOptions(opt);
  let overflow = 0;
  let lastOverflow = 0;
  let maxTimeRead = 0;
  let maxTimeWrite = 0;

  let hoursRule = new schedule.RecurrenceRule();
  //hoursRule.rule = '*/15 * * * * *';
  hoursRule.rule = '0 0 * * * *';

  let j = schedule.scheduleJob(hoursRule, () => {
    send({ id: 'settings', type: 'settings' }); //Get settings for retention policy
  });

  logger.log('Options: ' + JSON.stringify(options), 2);
  setInterval(async () => getDBSize(), 300000); //Get db size

  try {
    await client.createPoolToDatabase(options, logger);
    if (!client.pool) throw { message: 'Client creation Failed!' };

    await createTable('records');
    await client.query(`SELECT create_hypertable('records','ts', 
        chunk_time_interval => 86400000000, 
        if_not_exists => TRUE);`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_records_ts ON records (id, ts DESC);`);

    await createTable('timeline');
    await client.query(`CREATE INDEX IF NOT EXISTS idx_timeline_dn ON timeline (id, dn DESC);`);

    await createTable('customtable');
    await client.query(`CREATE INDEX IF NOT EXISTS namex_customtable ON customtable (name DESC);`);

    await createTable('formulas');

    getDBSize();

    channel.on('message', ({ id, type, query, payload, table }) => {
      if (type == 'write' && table == 'customtable') return writeCustom(id, payload, table);
      if (type == 'update') {
        return table == 'customtable' ? updateCustom(query, payload, table) : update(query, payload, table);
      }
      if (type == 'remove') return removeCustom(payload, table);

      if (type == 'write') {
        if (overflow == 0) return write(id, payload, table);
        if (overflow == 1 && lastOverflow == 0) {
          lastOverflow = overflow;
          return sendError(id, 'The allocated space for the database has run out, increase the limit');
        }
      }
      if (type == 'read') return read(id, query);
      if (type == 'settings') return del(payload);
      if (type == 'run') return run(id, query);
    });

    process.on('SIGTERM', () => {
      logger.log('Received SIGTERM');
      processExit(0);
    });

    process.on('exit', () => {
      if (client && client.pool) client.pool.close();
    });
  } catch (err) {
    processExit(1, err);
  }

  /**
   *
   * @param {String} tableName
   * @param {String} fname - optional
   */
  async function createTable(tableName) {
    return client.query(getCreateTableStr(tableName));
  }

  async function getDBSize() {
    let data = {};
    let sqlQuery = `SELECT hypertable_size('records') ;`;
    let dbSizeArr = await client.query(sqlQuery);
    //logger.log("DbSize " + util.inspect(dbSizeArr[0]), 2);
    let fileSize = dbSizeArr[0].hypertable_size / 1024 / 1024;
    data.size = parseInt(fileSize * 100) / 100;
    logger.log('Records Size ' + fileSize, 2);
    sqlQuery = `SELECT hypertable_size('timeline') ;`;
    dbSizeArr = await client.query(sqlQuery);
    fileSize = dbSizeArr[0].hypertable_size / 1024 / 1024;
    logger.log('Timeline Size ' + fileSize, 2);
    data.size += parseInt(fileSize * 100) / 100;

    if (process.connected) process.send({ type: 'procinfo', data: data });
    overflow = fileSize > opt.dbLimit ? 1 : 0;
    if (process.connected) process.send({ type: 'procinfo', data: { overflow: overflow } });
  }

  /**
   *
   * @param {String} id - request uuid
   * @param {Array of Objects} payload - [{ dn, prop, ts, val, id }]
   */
  async function write(id, payload, table) {
    const beginTime = Date.now();
    const tableName = table || 'records';
    const columns = getColumns(tableName);
    const values = utils.formValues(payload, columns);
    if (!values || !values.length) return;

    const query = 'INSERT INTO ' + tableName + ' (' + columns.join(',') + ') VALUES ';
    const values1 = values.map(i => `(${i})`).join(', ');
    let sql = query + ' ' + values1;
    logger.log('Sql ' + sql, 1);
    try {
      await client.query(sql);
      const endTime = Date.now();
      if (maxTimeWrite < endTime - beginTime) {
        maxTimeWrite = endTime - beginTime;
        if (process.connected)
          process.send({
            type: 'procinfo',
            data: { lastMaxTimeWrite: maxTimeWrite, lastMaxCountWrite: payload.length }
          });
      }
      logger.log('Write query id=' + id + util.inspect(payload), 2);
    } catch (err) {
      sendError(id, err);
    }
  }

  async function writeCustom(id, payload, table) {
    logger.log('writeCustom' + util.inspect(payload), 1);
    const columns = getColumns(table);
    const query = 'INSERT INTO ' + table + ' (' + columns.join(',') + ') VALUES ';
    const values = payload.map(i => `('${i.name}', ${i.ts}, '${i.payload}')`).join(', ');
    const sql = query + ' ' + values;
    logger.log('writeCustom sql ' + sql, 1);
    try {
      await client.query(sql);
    } catch (err) {
      sendError(id, err);
    }
  }

  async function updateCustom(query, payload, table) {
    logger.log('updateCustom ' + util.inspect(payload), 1); //[{id, $set:{field1:37, field2: 45}}]
    if (query.sql) {
      try {
        await client.query(query.sql);
      } catch (err) {
        sendError('update', err);
      }
    } else {
      for (j = 0; j < payload.length; j++) {
        const arr = Object.keys(payload[j]['$set']);
        for (i = 0; i < arr.length; i++) {
          const value = `JSONB_SET(payload, '{${arr[i]}}', ${
            typeof payload[j]['$set'][arr[i]] === 'string'
              ? `'"${payload[j]['$set'][arr[i]]}"')`
              : payload[j]['$set'][arr[i]]
          }`;
          const sql = `UPDATE ${table} SET payload =  ${value} WHERE ID = ${payload[j].id}`;
          logger.log('updateCustom' + sql);
          try {
            const changes = await client.query(sql);
            logger.log(`Row(s) updated ${changes}`, 2);
          } catch (err) {
            sendError('update', err);
          }
        }
      }
    }
  }

  /**
   * update
   * @param {Object} query
   * @param {Array of Objects} payload
   *     [{id, $set:{<field>:<val>, <field_json>:{<field1>:<val1>,..}, <field_json_with_arr>:{<field1[idx]>:<val1>,..} }}]
   *
   *        Ex1: [{id, $set:{active:1, description:'xxx'}}]
   *        Ex2: [{id, $set:{jhead:{BatchWeight:3000, ...}}}]
   *        Ex3: [{id, $set:{jrows:{OpType[0]:1,OpType[5]:1 }}}]
   * @param {String} table
   */
  async function update(query, payload, table) {
    logger.log('update ' + util.inspect(payload), 1);
    if (query.sql) {
      try {
        await client.query(query.sql);
      } catch (err) {
        sendError('update', err);
      }
    } else if (payload && payload.length) {
      for (j = 0; j < payload.length; j++) {
        const fieldsArr = Object.keys(payload[j]['$set']);
        const updateItems = getUpdateItems(fieldsArr);
        if (!updateItems.length) continue;

        const sql = `UPDATE ${table} SET ${updateItems.join(', ')}  WHERE id = ${payload[j].id}`;
        logger.log('sql ' + sql, 1);

        try {
          const changes = await client.query(sql);
          logger.log(`Row(s) updated ${changes}`, 2);
        } catch (err) {
          sendError('update', err);
        }
      }
    }

    function getUpdateItems(fieldsArr) {
      const updateItems = [];
      for (const field of fieldsArr) {
        const fieldVal = payload[j]['$set'][field];
        if (typeof fieldVal == 'object') {
          const arr = Object.keys(fieldVal);
          arr.forEach(prop => {
            updateItems.push(getJsonItem(field, prop, fieldVal[prop]));
          });
        } else {
          updateItems.push(getFieldSet(field, fieldVal));
        }
      }
      return updateItems;
    }
  }

  function getFieldSet(field, val) {
    return `${field} = '${val}'`; // TODO Для чисел - без кавычек!
  }

  /**
   *
   * @param {String} field
   * @param {String} prop
   * @param {String || Number} val
   * @return {String}
   *   ('jhead','BatchWeight', '500') => jhead['BatchWeight'] = '"500"'
   *   ('jhead','Flavor_Ref_Number', 142) => jhead['Flavor_Ref_Number'] = '142'
   *   ('jrows','OpType][3]', 1) =>  jrows['OpType'][3] = '1'
   *
   */
  function getJsonItem(field, prop, val) {
    let subprop = `['${prop}']`;
    if (prop.indexOf('[') > 0) {
      // OpType[3] => ['OpType'][3]
      const arr = prop.split('[');
      subprop = `['${arr[0]}'][${arr[1]}`;
    }
    const setval = typeof val === 'string' && field != 'jrows' ? `"${val}"` : `${Number(val)}`;
    return `${field}${subprop} = '${setval}'`;
  }

  async function removeCustom(payload, table) {
    logger.log('removeCustom' + util.inspect(payload), 1);
    const values = payload.map(i => `${i.id}`).join(', ');
    const sql = `DELETE FROM ${table} WHERE id IN (${values})`;
    try {
      const changes = await client.query(sql);
      logger.log(`Row(s) removed ${changes}`, 2);
    } catch (err) {
      sendError('remove', err);
    }
  }

  async function del(options) {
    let archDays = [1, 7, 15, 30, 90, 180, 360, 500];

    let tableName = 'records';
    for (const archDay of archDays) {
      let arrId = options.rp.filter(object => object.days == archDay);
      await deletePoints(tableName, archDay, arrId);
    }
  }

  async function deletePoints(tableName, archDay, arrId) {
    logger.log('Archday=' + archDay + ' ArrayofProps=' + JSON.stringify(arrId), 1);
    let archDepth = archDay * 86400000;
    //let archDepth = 600000;
    let delTime = Date.now() - archDepth;
    if (!arrId.length) return;
    while (arrId.length > 0) {
      let chunk = arrId.splice(0, 500);
      let values = chunk.map(i => `(id=${i.id})`).join(' OR ');
      logger.log('Map=' + values, 1);
      let sql = `DELETE FROM ${tableName} WHERE (${values}) AND ts<${delTime}`;
      try {
        const changes = await client.query(sql);
        logger.log(`Row(s) deleted ${changes}`, 1);
      } catch (err) {
        sendError('delete', err);
      }
    }
  }

  async function read(id, queryObj) {
    const beginTime = Date.now();
    let idarr, dnarr;
    try {
      logger.log('queryObj: ' + util.inspect(queryObj), 2);
      let queryStr;
      if (queryObj.sql) {
        queryStr = queryObj.sql;
      } else {
        if (!queryObj.dn_prop) throw { message: 'Expected dn_prop in query ' };
        if (queryObj.table == 'timeline') {
          dnarr = queryObj.dn_prop.split(',');
          queryStr = utils.getQueryStrDn(queryObj, dnarr);
        } else {
          idarr = queryObj.ids.split(',');
          queryStr = utils.getQueryStrId(queryObj, idarr);
        }
      }
      logger.log('SQL: ' + queryStr, 2);
      firstTime = Date.now();

      const result = await client.query(queryStr);
      //logger.log('Result: ' + util.inspect(result), 1);
      const endTime = Date.now();
      if (maxTimeRead < endTime - beginTime) {
        maxTimeRead = endTime - beginTime;
        if (process.connected)
          process.send({ type: 'procinfo', data: { lastMaxTimeRead: maxTimeRead, lastMaxCountRead: result.length } });
      }
      logger.log('Get result ' + id, 2);
      let payload = [];
      if (queryObj.sql || queryObj.table == 'timeline') {
        payload = result;
      } else {
        payload = queryObj.target == 'trend' ? formForTrend(result) : utils.recordsFor(result, queryObj);
      }

      logger.log('payload ' + util.inspect(payload), 2);
      send({ id, query: queryObj, payload });
    } catch (err) {
      sendError(id, err);
    }

    function formForTrend(res) {
      // return idarr.length == 1 ? res.map(item => [item.ts, Number(item.val)]) : utils.recordsForTrend(res, idarr);
      return idarr.length == 1 ? res.map(item => [item.ts, item.val]) : utils.recordsForTrend(res, idarr);
    }
  }

  function settings(id, query, payload) {
    logger.log('Recieve settings' + JSON.stringify(payload), 1);
    // if (query.loglevel) logger.setLoglevel(query.loglevel);
  }

  // NEW
  async function run(id, queryObj) {
    try {
      if (!queryObj.sql) throw { message: 'Expect sql clause!' };
      const sql = queryObj.sql;
      logger.log('run:' + util.inspect(sql), 1);
      await client.run(sql);
    } catch (err) {
      sendError(id, err);
    }
  }
  function send(message) {
    if (channel.connected) channel.send(message);
  }

  function sendError(id, err) {
    logger.log(err);
    send({ id, error: utils.getShortErrStr(err) });
  }

  function getOptions(argOpt) {
    //
    const res = {};

    return Object.assign(res, argOpt);
  }

  function processExit(code, err) {
    let msg = '';
    if (err) msg = 'ERROR: ' + utils.getShortErrStr(err) + ' ';

    if (client && client.pool) {
      client.pool.end();
      client.pool = null;
      msg += 'Close connection pool.';
    }

    logger.log(msg + ' Exit with code: ' + code);
    setTimeout(() => {
      channel.exit(code);
    }, 500);
  }
};

// Частные функции
// Строка для создания таблиц в БД
function getCreateTableStr(tableName) {
  let result;
  switch (tableName) {
    case 'timeline':
      result =
        'id serial PRIMARY KEY, ' +
        'dn text NOT NULL, prop text, ' +
        'start bigint NOT NULL, ' +
        '"end" bigint NOT NULL, ' +
        'state text';
      break;
    case 'customtable':
      result = 'id serial NOT NULL PRIMARY KEY, ' + 'name text NOT NULL, ' + 'ts bigint NOT NULL, ' + 'payload jsonb';
      break;
    case 'formulas':
      result =
        'id integer NOT NULL PRIMARY KEY, ' +
        'rid text NOT NULL, ' +
        'title text NOT NULL, ' +
        'description text, ' +
        'comments text, ' +
        'active integer, ' +
        'ts bigint, ' +
        'jhead jsonb, ' +
        'jrows jsonb';
      break;
    default:
      result = 'ts bigint NOT NULL, id integer, val real';
  }
  return 'CREATE TABLE IF NOT EXISTS ' + tableName + ' (' + result + ')';
}

function getColumns(tableName) {
  switch (tableName) {
    case 'timeline':
      return ['dn', 'prop', 'start', '"end"', 'state'];
    case 'customtable':
      return ['name', 'ts', 'payload'];
    case 'formulas':
      return ['id', 'rid', 'title', 'active', 'ts', 'description', 'comments', 'jhead', 'jrows'];
    default:
      return ['ts', 'id', 'val'];
  }
}
