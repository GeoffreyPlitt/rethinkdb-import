const fs = require('fs');
const path = require('path');
const _ = require('lodash');
const series = require('es6-promise-series');
const r = require('rethinkdbdash')();
const StreamArray = require('stream-json/streamers/StreamArray');
// ------------------------------- HELPERS -------------------------------------

const CHUNK_SIZE = 100;
const chunkData = (data) => {
  return data.reduce((arr, item, index) => {
    const chunkIndex = Math.floor(index / CHUNK_SIZE);
    if (!arr[chunkIndex]) {
      arr[chunkIndex] = []; // new chunk
    }
    arr[chunkIndex].push(item);
    return arr;
  }, []);
};

const handle_table = async (db_folder, table_file, table_list) => {
  return new Promise(async (resolve) => {
    const db_name = _.last(db_folder.split('/'));
    const table_name = _.last(table_file.split('/')).split('.')[0];

    console.log(`   Importing table ${table_file}...`);

    if (table_list.includes(table_name)) {
      await r.db(db_name).tableDrop(table_name).run(); //drop table, if exists, to clear data
    }
    await r.db(db_name).tableCreate(table_name).run(); // create table

    const fileStream = fs.createReadStream(table_file);
    const jsonStream = StreamArray.withParser();

    const jsonData = [];
    fileStream.pipe(jsonStream.input);
    jsonStream.on('data', ({ value }) => {
      jsonData.push(value);
    });
    jsonStream.on('end', async () => {
      console.log('Waiting for tables to be ready...');
      await r.db(db_name).wait();
      console.log('Writing data...');
      if (jsonData.length <= 100) {
        await r.db(db_name).table(table_name).insert(jsonData).run();
      } else {
        const chunked = chunkData(jsonData);
        for (const chunk of chunked) {
          await r.db(db_name).table(table_name).insert(chunk).run();
        }
      }
      console.log('Finished writing ');
      resolve();
    });
  });
};

const handle_db = async (db_folder) => {
  console.log(`Importing database ${db_folder}...`);
  const db_name = _.last(db_folder.split('/'));
  const table_list = await r.db(db_name).tableList().run();
  const file_names = (await fs.promises.readdir(db_folder)).filter((x) => _.endsWith(x, '.json'));
  const table_files = file_names.map((file_name) => path.join(db_folder, file_name));
  await Promise.all(
    // use promise.all to avoid closing program before all async processes finish
    table_files.map(async (table_file) => {
      await handle_table(db_folder, table_file, table_list);
    })
  );
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
