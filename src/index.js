import sqlite3 from "sqlite3";
import { faker } from "@faker-js/faker";
import sql from "./sql.js";

const db = new sqlite3.Database("my.db");

const ONE_MILLION = 1_000_000;
const TOTAL_SIZE = 1000 * ONE_MILLION;
const BATCH_SIZE = 10000;

function createRandomURLShortenerItem() {
  return {
    original_url: faker.internet.url(),
    short_code: faker.string.alpha(10),
  };
}

function generateBatchData(batchSize) {
  return faker.helpers.multiple(createRandomURLShortenerItem, {
    count: batchSize,
  });
}

async function createTable() {
  try {
    await sql.run(
      db,
      `CREATE TABLE IF NOT EXISTS url_shortener (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        original_url TEXT,
        short_code TEXT UNIQUE,
        created_at TEXT NOT NULL DEFAULT current_timestamp
      )`
    );
  } catch (error) {
    console.error(error);
  }
}

function insertBatch(items) {
  const values = items.map(() => `(?, ?)`);
  return new Promise((resolve, reject) => {
    const insertStatement = db.prepare(
      `INSERT INTO url_shortener (original_url, short_code) VALUES ${values}`
    );
    db.serialize(() => {
      db.run(`BEGIN TRANSACTION`, (err) => {
        if (err) {
          reject();
        }
      });
      insertStatement.run(
        items.map((item) => [item.original_url, item.short_code]).flat(),
        (err) => {
          if (err) {
            reject();
          }
        }
      );
      insertStatement.finalize((err) => {
        if (err) {
          reject();
        }
      });
      db.run(`COMMIT`, (err) => {
        if (err) {
          reject();
        } else {
          resolve();
        }
      });
    });
  });
}

async function main() {
  try {
    console.time("Create table");
    await createTable();
    console.timeEnd("Create table");

    console.time("insert");
    for (let i = 0; i < TOTAL_SIZE / BATCH_SIZE; i++) {
      const items = generateBatchData(BATCH_SIZE);
      await insertBatch(items);
      console.log(`Inserted ${i + 1}/ ${TOTAL_SIZE / BATCH_SIZE}`);
    }
    console.timeEnd("insert");

    // const data = await sql.fetchAll(
    //   db,
    //   `SELECT short_code from url_shortener WHERE original_url LIKE '%.com%' LIMIT 5`
    // );
    // const codes = data.map((d) => d.short_code);
  } catch (err) {
    console.error(err);
  }
}

main();
