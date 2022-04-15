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

    getDBSize();

    channel.on('message', ({ id, type, query, payload, table }) => {
      if (type == 'write') {
        if (overflow == 0) return write(id, payload, table);
        if (overflow == 1 && lastOverflow == 0) {
          lastOverflow = overflow;
          return sendError(id, 'The allocated space for the database has run out, increase the limit');
        }
      }
      if (type == 'read') return read(id, query);
      if (type == 'settings') return del(payload);
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
      let fileSize = dbSizeArr[0].hypertable_size/1024/1024;
      data.size = (parseInt(fileSize * 100)) / 100
      logger.log("Records Size " + fileSize, 2);
      sqlQuery = `SELECT hypertable_size('timeline') ;`; 
      dbSizeArr = await client.query(sqlQuery);
      fileSize = dbSizeArr[0].hypertable_size/1024/1024;
      logger.log("Timeline Size " + fileSize, 2);
      data.size += (parseInt(fileSize * 100)) / 100;

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
    // let sql = query + '('+ values1 + ')';
    let sql = query + ' ' + values1;
    // let values1 = values.map((value) => '(?)').join(',');
    // let sql = 'INSERT INTO records (ts,id,val) VALUES (1608734670896,2,20)'
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
    while (arrId.length>0) {
      let chunk = arrId.splice(0,500);
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
    const beginTime  = Date.now();
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
        if (process.connected) process.send({type:'procinfo', data:{lastMaxTimeRead: maxTimeRead, lastMaxCountRead:result.length}});
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
      return idarr.length == 1 ? res.map(item => [item.ts, Number(item.val)]) : utils.recordsForTrend(res, idarr);
    }
  }

  function settings(id, query, payload) {
    logger.log('Recieve settings' + JSON.stringify(payload), 1);
    // if (query.loglevel) logger.setLoglevel(query.loglevel);
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

    default:
      result = 'ts bigint NOT NULL, id integer, val real';
  }
  return 'CREATE TABLE IF NOT EXISTS ' + tableName + ' (' + result + ')';
}

function getColumns(tableName) {
  switch (tableName) {
    case 'timeline':
      return ['dn', 'prop', 'start', '"end"', 'state'];
    default:
      return ['ts', 'id', 'val'];
  }
}