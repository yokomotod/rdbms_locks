import mysql from "promise-mysql";
import pg from "pg";

type Db = "mysql" | "pg";

async function getConnection(db: Db) {
  switch (db) {
    case "mysql":
      return await mysql.createConnection({
        database: "test",
        host: process.env.MYSQL_HOST,
        user: process.env.MYSQL_USER || "root",
        password: process.env.MYSQL_PASSWORD,
        charset: "utf8mb4",
        timezone: "utc"
        // debug: true
      });
    case "pg":
      const client = new pg.Client({
        database: "test",
        host: process.env.PG_HOST,
        user: process.env.PG_USER || "postgres",
        password: process.env.PG_PASSWORD
      });
      await client.connect();
      return client;
    default:
      const _: never = db;
      throw new Error(_);
  }
}

async function query(
  connection: mysql.Connection | pg.Client,
  sql: string
): Promise<any[]> {
  if (connection instanceof pg.Client) {
    return (await connection.query(sql)).rows;
  } else {
    return await connection.query(sql);
  }
}
async function setup(db: Db) {
  let connection;

  try {
    connection = await getConnection(db);

    await query(connection, "drop table if exists test");
    await query(connection, "create table test (a int primary key, b int)");

    await query(connection, "insert into test values (1, 1)");
    await query(connection, "insert into test values (2, 2)");
  } finally {
    if (connection) {
      await connection.end();
    }
  }
}

type Level =
  | "repeatable read"
  | "read committed"
  | "read uncommitted"
  | "serializable";

async function beginTransaction(
  connection: mysql.Connection | pg.Client,
  level: Level
) {
  if (connection instanceof pg.Client) {
    await query(connection, "begin");
    await query(connection, `set transaction isolation level ${level}`);
  } else {
    await query(connection, "set innodb_lock_wait_timeout=0");
    await query(connection, `set transaction isolation level ${level}`);
    await query(connection, "begin");
  }
}

async function withConnections(
  db: Db,
  levelA: Level,
  levelB: Level,
  func: (
    connectionA: mysql.Connection | pg.Client,
    connectionB: mysql.Connection | pg.Client
  ) => Promise<void>
) {
  await setup(db);

  let connectionA;
  let connectionB;

  try {
    connectionA = await getConnection(db);
    connectionB = await getConnection(db);

    await beginTransaction(connectionA, levelA);
    await beginTransaction(connectionB, levelB);

    await func(connectionA, connectionB);
  } finally {
    if (connectionA) {
      await connectionA.end();
    }
    if (connectionB) {
      await connectionB.end();
    }
  }
}

type Expected = "consistent" | "inconsistent" | "error";

function assert(resultA: number, resultB: number, expected: Expected) {
  if (
    (resultA === resultB && expected === "inconsistent") ||
    (resultA !== resultB && expected === "consistent")
  ) {
    throw new Error(`${expected} is expected`);
  }
}

function checkError(error: Error, expected: Expected) {
  if (expected !== "error") {
    throw error;
  }

  if (error.message !== "could not serialize access due to concurrent update") {
    throw error;
  }
}

async function testDirtyRead(
  db: Db,
  levelA: Level,
  levelB: Level,
  expected: Expected
) {
  console.log(`testDirtyRead: ${db} ${levelA}/${levelB} => ${expected}`);
  await withConnections(
    db,
    levelA,
    levelB,
    async (connectionA, connectionB) => {
      const [result1] = await query(
        connectionA,
        "select * from test where a = 1"
      );

      await query(connectionB, "update test set b = 10 where a = 1");

      const [result2] = await query(
        connectionA,
        "select * from test where a = 1"
      );

      assert(result1.b, result2.b, expected);
    }
  );
}

async function testFuzzyRead(
  db: Db,
  levelA: Level,
  levelB: Level,
  expected: Expected
) {
  console.log(`testFuzzyRead: ${db} ${levelA}/${levelB} => ${expected}`);
  await withConnections(
    db,
    levelA,
    levelB,
    async (connectionA, connectionB) => {
      const [result1] = await query(
        connectionA,
        "select * from test where a = 1"
      );

      await query(connectionB, "update test set b = 10 where a = 1");
      await query(connectionB, "commit");

      const [result2] = await query(
        connectionA,
        "select * from test where a = 1"
      );

      assert(result1.b, result2.b, expected);
    }
  );
}

async function testFuzzyLockingRead(
  db: Db,
  levelA: Level,
  levelB: Level,
  expected: Expected
) {
  console.log(`testFuzzyLockingRead: ${db} ${levelA}/${levelB} => ${expected}`);
  await withConnections(
    db,
    levelA,
    levelB,
    async (connectionA, connectionB) => {
      const [result1] = await query(
        connectionA,
        "select * from test where a = 1"
      );

      await query(connectionB, "update test set b = 10 where a = 1");
      await query(connectionB, "commit");

      try {
        const [result2] = await query(
          connectionA,
          "select * from test where a = 1 for update"
        );

        assert(result1.b, result2.b, expected);
      } catch (e) {
        checkError(e, expected);
      }
    }
  );
}

async function testPhantomRead(
  db: Db,
  levelA: Level,
  levelB: Level,
  expected: Expected
) {
  console.log(`testPhantomRead: ${db} ${levelA}/${levelB} => ${expected}`);
  await withConnections(
    db,
    levelA,
    levelB,
    async (connectionA, connectionB) => {
      const results1 = await query(connectionA, "select * from test");

      await query(connectionB, "insert into test values (3, 3)");
      await query(connectionB, "commit");

      const results2 = await query(connectionA, "select * from test");

      assert(results1.length, results2.length, expected);
    }
  );
}

async function main() {
  await testDirtyRead(
    "mysql",
    "repeatable read",
    "repeatable read",
    "consistent"
  );
  await testDirtyRead(
    "mysql",
    "read committed",
    "repeatable read",
    "consistent"
  );
  await testDirtyRead(
    "mysql",
    "read uncommitted",
    "repeatable read",
    "inconsistent"
  );

  await testFuzzyRead(
    "mysql",
    "repeatable read",
    "repeatable read",
    "consistent"
  );
  await testFuzzyRead(
    "mysql",
    "read committed",
    "repeatable read",
    "inconsistent"
  );
  await testFuzzyRead(
    "mysql",
    "read uncommitted",
    "repeatable read",
    "inconsistent"
  );

  await testPhantomRead(
    "mysql",
    "repeatable read",
    "repeatable read",
    "consistent"
  );
  await testPhantomRead(
    "mysql",
    "read committed",
    "repeatable read",
    "inconsistent"
  );
  await testPhantomRead(
    "mysql",
    "read uncommitted",
    "repeatable read",
    "inconsistent"
  );

  // Locking ReadだとRepetable-ReadでもFuzzy Read
  await testFuzzyLockingRead(
    "mysql",
    "repeatable read",
    "repeatable read",
    "inconsistent"
  );
  await testFuzzyLockingRead(
    "mysql",
    "read committed",
    "repeatable read",
    "inconsistent"
  );
  await testFuzzyLockingRead(
    "mysql",
    "read uncommitted",
    "repeatable read",
    "inconsistent"
  );

  await testDirtyRead("pg", "repeatable read", "repeatable read", "consistent");
  await testDirtyRead("pg", "read committed", "repeatable read", "consistent");

  await testFuzzyRead("pg", "repeatable read", "repeatable read", "consistent");
  await testFuzzyRead(
    "pg",
    "read committed",
    "repeatable read",
    "inconsistent"
  );

  await testPhantomRead(
    "pg",
    "repeatable read",
    "repeatable read",
    "consistent"
  );
  await testPhantomRead(
    "pg",
    "read committed",
    "repeatable read",
    "inconsistent"
  );

  await testFuzzyLockingRead(
    "pg",
    "repeatable read",
    "repeatable read",
    "error"
  );
  await testFuzzyLockingRead(
    "pg",
    "read committed",
    "repeatable read",
    "inconsistent"
  );
}

main().catch(reason => {
  console.error(reason);
});
