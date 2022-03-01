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
  // hoursRule.rule = '*/15 * * * * *';
  hoursRule.rule = '0 0 * * * *';

  let j = schedule.scheduleJob(hoursRule, () => {
    send({ id: 'settings', type: 'settings' }); //Get settings for retention policy
  });

  logger.log('Options: ' + JSON.stringify(options), 2);
  setInterval(async () => getDBSize(), 60000); //Get db size

 

  try {
    await client.createPoolToDatabase(options, logger);
    if (!client.pool) throw { message: 'Client creation Failed!' };
    

    await createTable('records');
    await client.query(`SELECT create_hypertable('records','ts', 
        chunk_time_interval => 86400000000, 
        if_not_exists => TRUE);`);

    await createTable('timeline');

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
      //if (!fname) fname = tableName;
      //this.logger.log("Timeline: " + getCreateTableStr(tableName));
      return client.query(getCreateTableStr(tableName));
    }

    async function getDBSize() {
      const sqlQuery = `SELECT pg_size_pretty(pg_table_size('records'));`; 
      /*const sqlQuery = `SELECT table_name, pg_size_pretty( pg_relation_size(quote_ident(table_name)) )
      FROM information_schema.tables
      WHERE table_name = 'records'`;*/
      //logger.log("DbSize " + sqlQuery);
      const dbSizeArr = await client.query(sqlQuery);
      logger.log("DbSize " + util.inspect(dbSizeArr[0]), 2);
      let fileSize = parseInt(dbSizeArr[0].pg_size_pretty)/1024/1024;
      logger.log("DbSize " + fileSize, 2);
      if (process.connected) process.send({ type: 'procinfo', data: { size: fileSize } });
      overflow = fileSize > opt.dbLimit ? 1 : 0;
  
      if (process.connected) process.send({ type: 'procinfo', data: { overflow: overflow } });
    }

  /**
   *
   * @param {String} id - request uuid
   * @param {Array of Objects} payload - [{ dn, prop, ts, val }]
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
    // let sql = 'INSERT INTO records (dn,prop,ts,val) VALUES ("DT101","value",1608734670896,20)'
    //logger.log('Sql ' + values1);
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
      let arrDnProp = options.rp.filter(object => object.days == archDay);
      await deletePoints(tableName, archDay, arrDnProp);
    }

  }

  async function deletePoints(tableName, archDay, arrDnProp) {
    logger.log('Archday=' + archDay + ' ArrayofProps=' + JSON.stringify(arrDnProp), 1);
    let archDepth = archDay * 86400000;
    //let archDepth = 600000;
    let delTime = Date.now() - archDepth;
    if (!arrDnProp.length) return;
    while (arrDnProp.length>0) {
      let chunk = arrDnProp.splice(0,500);
      let values = chunk.map(i => `(dn='${i.dn}' AND prop='${i.prop}')`).join(' OR ');
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

    let dnarr;
    logger.log('Read query id=' + id + util.inspect(queryObj), 1);
    try {
      let queryStr;
      if (queryObj.sql) {
        queryStr = queryObj.sql;
      } else {
        if (!queryObj.dn_prop) throw { message: 'Expected dn_prop in query ' };
        dnarr = queryObj.dn_prop.split(',');
        queryStr = utils.getQueryStr(queryObj, dnarr);
      }
      logger.log('SQL: ' + queryStr, 1);
      firstTime = Date.now();

      const result = await client.query(queryStr);
      logger.log('Result: ' + util.inspect(result), 1);
       const endTime = Date.now();
      if (maxTimeRead < endTime - beginTime) {
        maxTimeRead = endTime - beginTime;
        if (process.connected) process.send({type:'procinfo', data:{lastMaxTimeRead: maxTimeRead, lastMaxCountRead:result.length}});
      }
      logger.log('Get result ' + id, 2);
      let payload = [];
      if (queryObj.data_type == 'calculation') {
        payload = await calculate(queryObj, dnarr, result);
      } else {
        payload = queryObj.target == 'trend' ? formForTrend(result) : result;
      }
      
      send({ id, query: queryObj, payload });
    } catch (err) {
      sendError(id, err);
    }

    function formForTrend(res) {
      return dnarr.length == 1 ? res.map(item => [item.ts, Number(item.val)]) : utils.recordsForTrend(res, dnarr);
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
        'id integer PRIMARY KEY NOT NULL, ' +
        'dn text NOT NULL, prop text, ' +
        'start integer NOT NULL, ' +
        '"end" integer NOT NULL, ' +
        'state text';
      break;

    default:
      result = 'ts bigint NOT NULL, dn text NOT NULL, prop text, val real';
  }
  return 'CREATE TABLE IF NOT EXISTS ' + tableName + ' (' + result + ')';
}

function getColumns(tableName) {
  switch (tableName) {
    case 'timeline':
      return ['dn', 'prop', 'start', 'end', 'state'];
    default:
      return ['dn', 'prop', 'ts', 'val'];
  }
}