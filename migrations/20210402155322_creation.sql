-- Your SQL goes here
CREATE TABLE IF NOT EXISTS jobs (
      id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
      name VARCHAR(256) NOT NULL,
      submit_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      status TINYINT NOT NULL DEFAULT 0 CHECK (status in (0, 1, 2, 3, 4, 5))
      -- status variants:
      -- * 0 -> Pending,
      -- * 1 -> Stopped,
      -- * 2 -> Killed,
      -- * 3 -> Failed,
      -- * 4 -> Succeed,
      -- * 5 -> Running,
);

CREATE TABLE IF NOT EXISTS tasks (
      id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
      name VARCHAR(256) NOT NULL DEFAULT "",
      handle VARCHAR(512) NOT NULL DEFAULT "",
      status TINYINT NOT NULL DEFAULT 0 CHECK (status in (0, 1, 2, 3, 4, 5)),
      command TEXT NOT NULL,
      job INTEGER,
      FOREIGN KEY(job) REFERENCES jobs(id) -- jobs pk constraint
);
CREATE TABLE IF NOT EXISTS task_dependencies (
      id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
      child INTEGER,
      parent INTEGER,
      FOREIGN KEY(child) REFERENCES tasks(id), -- tasks pk constraint
      FOREIGN KEY(parent) REFERENCES tasks(id) -- tasks pk constraint
);
