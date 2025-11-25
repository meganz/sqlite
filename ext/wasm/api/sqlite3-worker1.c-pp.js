//#if not omit-oo1
/*
  2022-05-23

  The author disclaims copyright to this source code.  In place of a
  legal notice, here is a blessing:

  *   May you do good and not evil.
  *   May you find forgiveness for yourself and forgive others.
  *   May you share freely, never taking more than you give.

  ***********************************************************************

  This is a JS Worker file for the main sqlite3 api. It loads
  sqlite3.js, initializes the module, and postMessage()'s a message
  after the module is initialized:

  {type: 'sqlite3-api', result: 'worker1-ready'}

  This seemingly superfluous level of indirection is necessary when
  loading sqlite3.js via a Worker. Instantiating a worker with new
  Worker("sqlite.js") will not (cannot) call sqlite3InitModule() to
  initialize the module due to a timing/order-of-operations conflict
  (and that symbol is not exported in a way that a Worker loading it
  that way can see it).  Thus JS code wanting to load the sqlite3
  Worker-specific API needs to pass _this_ file (or equivalent) to the
  Worker constructor and then listen for an event in the form shown
  above in order to know when the module has completed initialization.

  This file accepts a URL arguments to adjust how it loads sqlite3.js:

  - `sqlite3.dir`, if set, treats the given directory name as the
    directory from which `sqlite3.js` will be loaded.
*/
//#if target:es6-bundler-friendly
import {default as sqlite3InitModule} from './sqlite3-bundler-friendly.mjs';
//#elif target:es6-module
    return new Worker(new URL("sqlite3.js", import.meta.url));
//#else
"use strict";
{
  const urlParams = globalThis.location
        ? new URL(globalThis.location.href).searchParams
        : new URLSearchParams();
  let theJs = 'sqlite3.js';
  if(urlParams.has('sqlite3.dir')){
    theJs = urlParams.get('sqlite3.dir') + '/' + theJs;
  }
  //console.warn("worker1 theJs =",theJs);
  importScripts(theJs);
}
//#endif
sqlite3InitModule().then(sqlite3 => {
  const _installOpfsPool = async () => {

    try {
      return await sqlite3.installOpfsSAHPoolVfs({
        vfsName: 'opfs-sahpool',
        initialCapacity: 6,
        clearOnInit: false,
        directory: 'sqlite-sahpool-dir'
      });
    }
    catch (ex) {
      postMessage({type: 'worker-init-failed', error: ex.message});
    }
  };

  _installOpfsPool().then(poolutil => {

    postMessage({type: 'worker-init-success'});

    sqlite3.initWorker1API(

      // Mega Webclient only, extended API
      function apiExt({wMsgHandler, getMsgDb}) {

        const ext = Object.create(null);
        const origOpen = wMsgHandler.open;
        const origClose = wMsgHandler.close;
        let db;

        if (poolutil) {
          wMsgHandler.open = function(ev) {

            const fn = poolutil.getFileNames();

            // Clear out any non current files
            if (fn.length) {
              for (let i = fn.length; i--;) {
                if (fn[i] !== ev.args.filename.replace('file:', '/')) {
                  poolutil.unlink(fn[i]);
                }
              }
            }

            db = origOpen(ev);

            return db;
          };
        }

        wMsgHandler.close = function(ev) {

          console.log('[FMDB][SQLite] Worker1 API close called');

          if (db && db.pointer) {
            sqlite3.capi.sqlite3_interrupt(db.pointer);
          }

          return origClose(ev);
        };

        self.addEventListener('close', wMsgHandler.close);

        wMsgHandler.bulkput = function(ev) {

          const args = ev.args || Object.create(null);
          const table = args.table;
          const columns = Array.isArray(args.columns) ? args.columns : [];
          const binds = Array.isArray(args.binds) ? args.binds : [];
          const skipTx = !!args.skipTx;
          const db = getMsgDb(ev);

          if (!table || !columns.length) {
            throw new Error("bulkput requires table and columns");
          }
          if (!binds.length) {
            return {ok: true, execCount: 0};
          }

          const escapeId = (s) => String(s).replace(/[^A-Za-z0-9_]/g, '_');
          const perRowBind = columns.length;
          if (binds.length % perRowBind !== 0) {
            throw new Error('bulkput binds length mismatch');
          }
          const tableName = escapeId(table);
          const colSql = columns.map(c => escapeId(c)).join(',');
          const rowPlace = '(' + new Array(perRowBind).fill('?').join(',') + ')';

          let committed = false;
          let startedTxn = false;
          if (!skipTx) {
            db.exec('BEGIN IMMEDIATE');
          }
          startedTxn = true;

          const totalRows = Math.floor(binds.length / perRowBind);
          const sql = `INSERT OR REPLACE INTO ${tableName} (${colSql}) VALUES ${rowPlace}`;
          const stmt = db.prepare(sql);

          for (let rowIdx = 0; rowIdx < totalRows; rowIdx++) {

            const start = rowIdx * perRowBind;
            const end = start + perRowBind;

            stmt.bind(binds.slice(start, end));
            stmt.step();
            stmt.reset(true);
          }
          stmt.finalize();

          if (!skipTx && startedTxn) {
            db.exec('COMMIT');
            committed = true;
          }

          if (!skipTx && startedTxn && !committed) {
            db.exec('ROLLBACK');
          }

          return {ok: true, execCount: Math.floor(binds.length / perRowBind)};
        };
        return ext;
      });
  });
});
//#else
/* Built with the omit-oo1 flag. */
//#endif if not omit-oo1
