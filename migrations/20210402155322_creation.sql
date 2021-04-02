-- Your SQL goes here
CREATE TABLE jobs (
      id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
      name VARCHAR(256) NOT NULL,
      submit_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      status INTEGER NOT NULL DEFAULT 0 CHECK (status in (0, 1, 2, 3))
      -- status variants:
      -- * 0 -> pending
      -- * 1 -> running
      -- * 2 -> failed
      -- * 3 -> finished
);
CREATE TABLE processes (
      id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
      pid INTEGER DEFAULT NULL,
      stdout TEXT,
      stderr TEXT
);
CREATE TABLE tasks (
      id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
      name VARCHAR(256) NOT NULL,
      status INTEGER NOT NULL DEFAULT 0 CHECK (status in (0, 1, 2, 3)),
      command TEXT NOT NULL,
      process INTEGER,
      job INTEGER,
      FOREIGN KEY(process) REFERENCES processes(id), -- process pk constraint
      FOREIGN KEY(job) REFERENCES jobs(id) -- jobs pk constraint
);
CREATE TABLE task_dependencies (
      id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
      child INTEGER,
      parent INTEGER,
      FOREIGN KEY(child) REFERENCES tasks(id), -- tasks pk constraint
      FOREIGN KEY(parent) REFERENCES tasks(id) -- tasks pk constraint
);
