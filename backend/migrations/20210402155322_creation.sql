-- Your SQL goes here
CREATE TABLE IF NOT EXISTS jobs (
      id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
      name VARCHAR(256) NOT NULL,
      submit_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      status TINYINT NOT NULL DEFAULT 0 CHECK (status in (0, 1, 2, 3, 4, 5, 6))
      -- status variants:
      -- * 0 -> Pending,
      -- * 1 -> Stopped,
      -- * 2 -> Killed,
      -- * 3 -> Failed,
      -- * 4 -> Succeed,
      -- * 5 -> Running,
      -- * 6 -> Canceled,
);

-- Single task (eg shell command)
CREATE TABLE IF NOT EXISTS tasks (
      id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
      name VARCHAR(256) NOT NULL DEFAULT "",
      handle VARCHAR(512) NOT NULL DEFAULT "",
      status TINYINT NOT NULL DEFAULT 0 CHECK (status in (0, 1, 2, 3, 4, 5, 6)),
      stderr TEXT DEFAULT NULL,
      stdout TEXT DEFAULT NULL,
      job INTEGER,
      FOREIGN KEY(job) REFERENCES jobs(id) -- jobs pk constraint
);

CREATE TABLE IF NOT EXISTS task_command_arguments (
      id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
      argument TEXT NOT NULL,
      position INTEGER,
      task INTEGER,
      FOREIGN KEY(task) REFERENCES tasks(id) -- tasks pk constraint
);

CREATE TABLE IF NOT EXISTS task_dependencies (
      id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
      child INTEGER,
      parent INTEGER,
      job INTEGER,
      FOREIGN KEY(child) REFERENCES tasks(id), -- tasks pk constraint
      FOREIGN KEY(parent) REFERENCES tasks(id) -- tasks pk constraint
      FOREIGN KEY(job) REFERENCES jobs(id) -- tasks pk constraint
);
