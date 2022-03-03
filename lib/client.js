/**
 * postgresql client
 */

const {Pool, types} = require('pg');
types.setTypeParser(20, (val) => parseInt(val));

module.exports = {
  pool: null,

  async createPoolToDatabase(dbopt) {
    this.pool = new Pool(dbopt);
  },

  run(sql, values) {
    return new Promise((resolve, reject) => {
      this.pool.query(sql, [values], (err, res) => {
        if (!err) {
          resolve(res);
        } else reject(err);
      });
    });
  },

  query(sql) {
    return new Promise((resolve, reject) => {
      this.pool.query({text: sql}, (err, res) => {
        if (!err) {  
          resolve(res.rows);
        } else reject(err);
      });
    });
  },

};
