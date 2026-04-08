'use strict';

/**
 * sshService.js — SFTP wrapper over the ssh2 library.
 *
 * This is the ONLY file in the project that imports 'ssh2'.
 * Every other module depends on this abstraction, not on the library directly.
 * If the underlying SSH library changes, only this file needs to be updated.
 */

const { Client } = require('ssh2');
const fs         = require('fs');
const path       = require('path');
const os         = require('os');

// ── Helpers ───────────────────────────────────────────────────────────────────

/**
 * Resolves '~/' prefix to the actual home directory.
 * @param {string} filePath
 * @returns {string}
 */
function resolvePath(filePath) {
  if (filePath.startsWith('~/')) {
    return path.join(os.homedir(), filePath.slice(2));
  }
  return filePath;
}

// ── Public API ────────────────────────────────────────────────────────────────

/**
 * Opens an SSH connection and returns a paired { client, sftp } handle.
 * The caller is responsible for calling disconnect(client) when done.
 *
 * @param {{ host: string, port: number, username: string, privateKeyPath: string }} nodeConfig
 * @returns {Promise<{ client: Client, sftp: object }>}
 */
function connect(nodeConfig) {
  return new Promise((resolve, reject) => {
    const client = new Client();

    const privateKeyPath = resolvePath(nodeConfig.privateKeyPath);
    let privateKey;
    try {
      privateKey = fs.readFileSync(privateKeyPath);
    } catch (err) {
      return reject(new Error(
        `[sshService] Cannot read private key at "${privateKeyPath}": ${err.message}`
      ));
    }

    client.on('ready', () => {
      client.sftp((err, sftp) => {
        if (err) {
          client.end();
          return reject(new Error(`[sshService] SFTP subsystem error on ${nodeConfig.host}: ${err.message}`));
        }
        resolve({ client, sftp });
      });
    });

    client.on('error', (err) => {
      reject(new Error(`[sshService] Connection failed to ${nodeConfig.host}: ${err.message}`));
    });

    client.connect({
      host:       nodeConfig.host,
      port:       nodeConfig.port || 22,
      username:   nodeConfig.username,
      privateKey,
      readyTimeout: 10_000,
    });
  });
}

/**
 * Lists the contents of a remote directory.
 * Returns an empty array if the directory does not exist.
 *
 * @param {object} sftp - Active SFTP session from connect()
 * @param {string} remotePath - Absolute remote path
 * @returns {Promise<Array<{ filename: string, longname: string, attrs: object }>>}
 */
function listDir(sftp, remotePath) {
  return new Promise((resolve, reject) => {
    sftp.readdir(remotePath, (err, list) => {
      if (err) {
        // ENOENT means the directory simply doesn't exist on this node — not fatal
        if (err.code === 2) return resolve([]);
        return reject(new Error(`[sshService] listDir failed at "${remotePath}": ${err.message}`));
      }
      resolve(list);
    });
  });
}

/**
 * Reads a remote file and returns its content as a UTF-8 string.
 * Returns null if the file does not exist.
 *
 * @param {object} sftp - Active SFTP session from connect()
 * @param {string} remotePath - Absolute remote path
 * @returns {Promise<string|null>}
 */
function readFileAsString(sftp, remotePath) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    const stream = sftp.createReadStream(remotePath);

    stream.on('error', (err) => {
      if (err.code === 2) return resolve(null); // ENOENT — file not found
      reject(new Error(`[sshService] readFile failed at "${remotePath}": ${err.message}`));
    });
    stream.on('data', (chunk) => chunks.push(chunk));
    stream.on('close', () => resolve(Buffer.concat(chunks).toString('utf8')));
  });
}

/**
 * Returns a Readable stream for a remote file.
 * Use this for large files (like JSONL sessions) to avoid loading everything in memory.
 *
 * @param {object} sftp - Active SFTP session from connect()
 * @param {string} remotePath - Absolute remote path
 * @returns {Readable}
 */
function createReadStream(sftp, remotePath) {
  return sftp.createReadStream(remotePath);
}

/**
 * Writes a string to a remote file.
 * 
 * @param {object} sftp - Active SFTP session from connect()
 * @param {string} remotePath - Absolute remote path
 * @param {string} content - File content
 * @returns {Promise<boolean>}
 */
function writeFileAsString(sftp, remotePath, content) {
  return new Promise((resolve, reject) => {
    const stream = sftp.createWriteStream(remotePath);
    stream.on('error', (err) => reject(new Error(`[sshService] writeFile failed at "${remotePath}": ${err.message}`)));
    stream.on('close', () => resolve(true));
    stream.write(content, 'utf8');
    stream.end();
  });
}

/**
 * Gracefully closes an SSH connection.
 * @param {Client} client
 */
function disconnect(client) {
  try { client.end(); } catch (_) { /* already closed */ }
}

module.exports = { connect, listDir, readFileAsString, writeFileAsString, createReadStream, disconnect };
