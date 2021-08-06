const fs = require('fs');
const path = require('path');
const _ = require('lodash');
const series = require('es6-promise-series');
const r = require('rethinkdbdash')();
const StreamArray = require('stream-json/streamers/StreamArray');
const { Writable } = require('stream');
// ------------------------------- HELPERS -------------------------------------

const handle_table = async (db_folder, table_file) => {
  const db_name = _.last(db_folder.split('/'));
  const table_name = _.last(table_file.split('/')).split('.')[0];
  console.log(`   Importing table ${table_file}...`);

  await r.db(db_name).tableCreate(table_name).run(); // create table

  let jsonData = [];

  const fileStream = fs.createReadStream(table_file);
  const jsonStream = StreamArray.withParser();

  const processingStream = new Writable({
    write({ key, value }, encoding, callback) {
      if (jsonData.length === 99) {
        r.db(db_name)
          .table(table_name)
          .insert(jsonData)
          .run()
          .then(() => {
            jsonData = [];
          });
      }
      jsonData.push(value);
      callback();
    },
    objectMode: true,
  });
  fileStream.pipe(jsonStream.input);
  jsonStream.pipe(processingStream);

  processingStream.on('finish', () => {
    r.db(db_name).table(table_name).insert(jsonData).run();
  });
};

const handle_db = async (db_folder) => {
  console.log(`Importing database ${db_folder}...`);
  const db_name = _.last(db_folder.split('/'));
  await r.dbDrop(db_name);
  await r.dbCreate(db_name);
  const file_names = (await fs.promises.readdir(db_folder)).filter((x) => _.endsWith(x, '.json'));
  const table_files = file_names.map((file_name) => path.join(db_folder, file_name));
  await series(table_files.map((table_file) => () => handle_table(db_folder, table_file)));
};

// --------------------------------- MAIN --------------------------------------
const main = async () => {
  try {
    const root_folder = process.argv[2];
    if (!fs.existsSync(root_folder)) {
      console.error(`The path does not exist (${root_folder})`);
      process.exit(1);
    }
    // Now we know the path exists. Look at contents of that root folder
    const file_names = (await fs.promises.readdir(root_folder)).filter(
      (x) => !x.includes('.DS_Store')
    );
    const db_folders = file_names.map((file_name) => path.join(root_folder, file_name));

    // handle each DB folder
    await series(db_folders.map((db_folder) => () => handle_db(db_folder)));
    console.log('Done!');
    process.exit(0);
  } catch (err) {
    console.error(`ERROR: ${err}`);
  }
};

main();
